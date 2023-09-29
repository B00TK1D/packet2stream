package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoURI = flag.String("m", "mongodb://localhost:27017", "Mongodb URI")
var dbName = flag.String("d", "traffic", "Mongodb database name")
var packetColName = flag.String("c", "packets", "Mongodb collection name for packets")
var streamColName = flag.String("s", "streams", "Mongodb collection name for streams")
var streamTimeout = flag.Int64("t", 60, "Stream timeout in seconds")

var ctxTodo = context.TODO()

var database *mongo.Database
var packetCol *mongo.Collection
var streamCol *mongo.Collection

const MaxInt64 = int64(^uint64(0) >> 1)

// Stream struct
type Stream struct {
	TimeStart        int64
	TimeEnd          int64
	AckReceived      bool
	FinReceived      bool
	ClientIp         string
	ServerIp         string
	ClientPort       int64
	ServerPort       int64
	Protocol         string
	ClientPayloads   [][]byte
	ServerPayloads   [][]byte
	ClientPayloadSeq []int64
	ServerPayloadSeq []int64
}

func createStream(stream Stream, packetId primitive.ObjectID) {
	// Insert the stream
	insertResult, err := streamCol.InsertOne(ctxTodo, stream)
	if err != nil {
		log.Fatal(err)
	}

	// Get the last inserted object id
	streamId := insertResult.InsertedID.(primitive.ObjectID)

	// Set the packet's stream
	_, err = packetCol.UpdateOne(ctxTodo, bson.M{"_id": packetId}, bson.M{"$set": bson.M{"stream": streamId}})
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// Read in cmd line args
	flag.Parse()
	streamTimeoutMilliseconds := *streamTimeout * int64(1000000000)

	// Connect to MongoDB
	client, err := mongo.Connect(ctxTodo, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctxTodo)
	database = client.Database(*dbName)
	packetCol = database.Collection(*packetColName)
	streamCol = database.Collection(*streamColName)

	// Get all packets without streams
	// packetCursor, err := packetCol.Find(ctxTodo, bson.M{"stream": bson.M{"$exists": false}})
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{Key: "timestamp", Value: 1}})
	packetCursor, err := packetCol.Find(ctxTodo, bson.M{}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Iterate over packets
	for packetCursor.Next(ctxTodo) {
		// Check if the packet matches an existing stream
		var packet bson.M = make(map[string]interface{})
		err := packetCursor.Decode(&packet)
		if err != nil {
			log.Fatal(err)
		}

		var serverIp string
		var clientIp string
		var serverPort int64
		var clientPort int64
		var fromClient bool
		var payload []byte

		// Check direction of packet - the simple way to do this is compare the port numbers (if they're equal, arbitrarily choose the lower IP address to avoid collisions)
		if packet["srcport"].(int64) < packet["dstport"].(int64) || packet["srcport"].(int64) == packet["dstport"].(int64) && packet["srcip"].(string) < packet["dstip"].(string) {
			clientIp = packet["dstip"].(string)
			serverIp = packet["srcip"].(string)
			clientPort = packet["dstport"].(int64)
			serverPort = packet["srcport"].(int64)
			fromClient = false
		} else {
			clientIp = packet["srcip"].(string)
			serverIp = packet["dstip"].(string)
			clientPort = packet["srcport"].(int64)
			serverPort = packet["dstport"].(int64)
			fromClient = true
		}

		packetId := packet["_id"].(primitive.ObjectID)
		timestamp := packet["timestamp"].(int64)
		protocol := packet["protocol"].(string)
		tcpflags := packet["tcpflag"].(bson.M)
		payload = packet["payload"].(primitive.Binary).Data
		seq := packet["tcpseq"].(int64)

		streamCursor, err := streamCol.Find(ctxTodo, bson.M{"clientip": clientIp, "serverip": serverIp, "clientport": clientPort, "serverport": serverPort, "protocol": protocol, "timestart": bson.M{"$lt": timestamp}, "timeend": bson.M{"$gt": timestamp}})

		// If the packet matches an existing stream, update the stream
		if err == nil {
			var stream bson.M = make(map[string]interface{})
			streamCursor.Next(ctxTodo)
			err = streamCursor.Decode(&stream)

			// Check if stream exists
			if err == mongo.ErrNoDocuments || stream["_id"] == nil {
				// Create a new stream
				stream := Stream{
					TimeStart:        timestamp - streamTimeoutMilliseconds,
					TimeEnd:          timestamp + streamTimeoutMilliseconds,
					AckReceived:      false,
					FinReceived:      false,
					ClientIp:         clientIp,
					ServerIp:         serverIp,
					ClientPort:       clientPort,
					ServerPort:       serverPort,
					Protocol:         protocol,
					ClientPayloads:   [][]byte{},
					ServerPayloads:   [][]byte{},
					ClientPayloadSeq: []int64{},
					ServerPayloadSeq: []int64{},
				}

				if fromClient {
					stream.ClientPayloads = append(stream.ClientPayloads, payload)
					stream.ClientPayloadSeq = append(stream.ClientPayloadSeq, packet["tcpseq"].(int64))
				} else {
					stream.ServerPayloads = append(stream.ServerPayloads, payload)
					stream.ServerPayloadSeq = append(stream.ServerPayloadSeq, packet["tcpseq"].(int64))
				}

				if tcpflags["fin"] == true || tcpflags["rst"] == true {
					stream.TimeEnd = timestamp
					stream.FinReceived = true
				}

				if tcpflags["ack"] == true {
					stream.TimeStart = timestamp
					stream.AckReceived = true
				}

				createStream(stream, packetId)
				continue
			}

			streamUpdate := bson.M{}
			streamPush := bson.M{}
			streamId := stream["_id"].(primitive.ObjectID)

			if tcpflags["fin"] == true || tcpflags["rst"] == true {
				streamUpdate["timeend"] = timestamp
				streamUpdate["finreceived"] = true
			} else if tcpflags["ack"] == true {
				streamUpdate["timestart"] = timestamp
				streamUpdate["ackreceived"] = true
			} else {
				if timestamp+streamTimeoutMilliseconds > stream["timeend"].(int64) {
					streamUpdate["lasttime"] = timestamp + streamTimeoutMilliseconds
				}
				if timestamp-streamTimeoutMilliseconds < stream["timestart"].(int64) {
					streamUpdate["firsttime"] = timestamp - streamTimeoutMilliseconds
				}
			}

			// Append the payload to the stream
			if len(payload) > 0 {

				clientPayloads := primitive.A{}
				serverPayloads := primitive.A{}

				// Key assumption:
				// The client sends the first payload in the conversation
				// If this is not true, we add an initial empty payload from the client (so we can make client ans server payloads line up)
				if stream["clientpayloads"] != nil {
					clientPayloads = stream["clientpayloads"].(primitive.A)
				} else if !fromClient {
					streamPush["clientpayloads"] = []byte{}
				}
				if stream["serverpayloads"] != nil {
					serverPayloads = stream["serverpayloads"].(primitive.A)
				}

				lastFromClient := len(clientPayloads) > len(serverPayloads)
				if fromClient {
					// Check if last message was also from client
					if !lastFromClient || len(clientPayloads) == 0 {
						// Add a new payload
						streamPush["clientpayloads"] = payload
						streamPush["clientpayloadseq"] = seq
					} else {
						// Concatenate the last payload with the new payload
						payloadIndex := len(clientPayloads) - 1

						existingPayload := clientPayloads[payloadIndex].(primitive.Binary).Data
						existingStart := stream["clientpayloadseq"].(primitive.A)[payloadIndex].(int64)
						existingEnd := existingStart + int64(len(existingPayload))
						newPayload := payload
						newStart := seq
						newEnd := newStart + int64(len(newPayload))
						var mergedStart int64
						var mergedEnd int64
						var mergedPayload []byte

						if existingStart < newStart {
							mergedStart = existingStart
						} else {
							mergedStart = newStart
						}
						if existingEnd > newEnd {
							mergedEnd = existingEnd
						} else {
							mergedEnd = newEnd
						}
						mergedPayload = make([]byte, mergedEnd-mergedStart)
						copy(mergedPayload[existingStart-mergedStart:], existingPayload)
						copy(mergedPayload[newStart-mergedStart:], newPayload)
						streamUpdate[fmt.Sprintf("clientpayloads.%d", payloadIndex)] = mergedPayload
						streamUpdate[fmt.Sprintf("clientpayloadseq.%d", payloadIndex)] = mergedStart
					}
				} else {
					// Check if last message was also from server
					if lastFromClient || len(serverPayloads) == 0 {
						// Append to the last payload
						streamPush["serverpayloads"] = payload
						streamPush["serverpayloadseq"] = seq
					} else {
						// Concatenate the last payload with the new payload
						payloadIndex := len(serverPayloads) - 1

						existingPayload := serverPayloads[payloadIndex].(primitive.Binary).Data
						existingStart := stream["serverpayloadseq"].(primitive.A)[payloadIndex].(int64)
						existingEnd := existingStart + int64(len(existingPayload))
						newPayload := payload
						newStart := seq
						newEnd := newStart + int64(len(newPayload))
						var mergedStart int64
						var mergedEnd int64
						var mergedPayload []byte

						if existingStart < newStart {
							mergedStart = existingStart
						} else {
							mergedStart = newStart
						}
						if existingEnd > newEnd {
							mergedEnd = existingEnd
						} else {
							mergedEnd = newEnd
						}
						mergedPayload = make([]byte, mergedEnd-mergedStart)
						copy(mergedPayload[existingStart-mergedStart:], existingPayload)
						copy(mergedPayload[newStart-mergedStart:], newPayload)
						streamUpdate[fmt.Sprintf("serverpayloads.%d", payloadIndex)] = mergedPayload
						streamUpdate[fmt.Sprintf("serverpayloadseq.%d", payloadIndex)] = mergedStart
					}
				}
			}

			// Update the stream
			_, err = streamCol.UpdateOne(ctxTodo, bson.M{"_id": streamId}, bson.M{"$set": streamUpdate, "$push": streamPush})
			if err != nil {
				log.Fatal(err)
			}

			// Set the packet's stream
			_, err = packetCol.UpdateOne(ctxTodo, bson.M{"_id": packetId}, bson.M{"$set": bson.M{"stream": streamId}})
			if err != nil {
				log.Fatal(err)
			}
		} else {
			// If the packet does not match an existing stream, create a new stream
			stream := Stream{
				TimeStart:        timestamp - streamTimeoutMilliseconds,
				TimeEnd:          timestamp + streamTimeoutMilliseconds,
				AckReceived:      false,
				FinReceived:      false,
				ClientIp:         clientIp,
				ServerIp:         serverIp,
				ClientPort:       clientPort,
				ServerPort:       serverPort,
				Protocol:         protocol,
				ClientPayloads:   [][]byte{},
				ServerPayloads:   [][]byte{},
				ClientPayloadSeq: []int64{},
				ServerPayloadSeq: []int64{},
			}

			if fromClient {
				stream.ClientPayloads = append(stream.ClientPayloads, payload)
				stream.ClientPayloadSeq = append(stream.ClientPayloadSeq, packet["tcpseq"].(int64))
			} else {
				stream.ServerPayloads = append(stream.ServerPayloads, payload)
				stream.ServerPayloadSeq = append(stream.ServerPayloadSeq, packet["tcpseq"].(int64))
			}

			if tcpflags["fin"] == true || tcpflags["rst"] == true {
				stream.TimeEnd = timestamp
				stream.FinReceived = true
			}

			if tcpflags["ack"] == true {
				stream.TimeStart = timestamp
				stream.AckReceived = true
			}

			createStream(stream, packetId)
		}
	}
	// Remove any streams with an empty payload

	// Get all streams with a payload equal to "" binary
	_, err = streamCol.DeleteMany(ctxTodo, bson.M{"payload": primitive.Binary{}})
	if err != nil {
		log.Fatal(err)
	}
}
