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

// Stream struct
type Stream struct {
	FirstTime      int64
	LastTime       int64
	Terminated     bool
	ClientIp       string
	ServerIp       string
	ClientPort     int64
	ServerPort     int64
	Protocol       string
	BaseSeq        int64
	ClientPayloads [][]byte
	ServerPayloads [][]byte
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
	packetCursor, err := packetCol.Find(ctxTodo, bson.M{})
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

		streamCursor, err := streamCol.Find(ctxTodo, bson.M{"terminated": false, "clientip": clientIp, "serverip": serverIp, "clientport": clientPort, "serverport": serverPort, "protocol": protocol, "lasttime": bson.M{"$lt": timestamp + streamTimeoutMilliseconds}})

		// If the packet matches an existing stream, update the stream
		if err == nil {
			var stream bson.M = make(map[string]interface{})
			streamCursor.Next(ctxTodo)
			err = streamCursor.Decode(&stream)

			// Check if stream exists
			if err == mongo.ErrNoDocuments || stream["_id"] == nil {
				// Create a new stream
				stream := Stream{
					FirstTime:      timestamp,
					LastTime:       timestamp,
					Terminated:     false,
					ClientIp:       clientIp,
					ServerIp:       serverIp,
					ClientPort:     clientPort,
					ServerPort:     serverPort,
					Protocol:       protocol,
					BaseSeq:        packet["tcpseq"].(int64),
					ClientPayloads: [][]byte{},
					ServerPayloads: [][]byte{},
				}
				if fromClient {
					stream.ClientPayloads = append(stream.ClientPayloads, payload)
				} else {
					stream.ServerPayloads = append(stream.ServerPayloads, payload)
				}

				createStream(stream, packetId)
				continue
			}

			streamUpdate := bson.M{}
			streamPush := bson.M{}
			streamId := stream["_id"].(primitive.ObjectID)

			if protocol == "tcp" {
				if tcpflags["fin"] == true || tcpflags["rst"] == true {
					streamUpdate["terminated"] = true
				}
			}

			// Append the payload to the stream
			if len(payload) > 0 {

				clientPayloads := primitive.A{}
				serverPayloads := primitive.A{}

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
					} else {
						// Append to the last payload
						// Concatenate the last payload with the new payload
						newPayload := make([]byte, len(clientPayloads[len(clientPayloads)-1].(primitive.Binary).Data)+len(payload))
						copy(newPayload, clientPayloads[len(clientPayloads)-1].(primitive.Binary).Data)
						copy(newPayload[len(clientPayloads[len(clientPayloads)-1].(primitive.Binary).Data):], payload)
						streamUpdate[fmt.Sprintf("clientpayloads.%d", len(clientPayloads)-1)] = newPayload
					}
				} else {
					// Check if last message was also from server
					if lastFromClient || len(serverPayloads) == 0 {
						// Append to the last payload
						streamPush["serverpayloads"] = payload
					} else {
						// Add a new payload
						// Concatenate the last payload with the new payload
						newPayload := make([]byte, len(serverPayloads[len(serverPayloads)-1].(primitive.Binary).Data)+len(payload))
						copy(newPayload, serverPayloads[len(serverPayloads)-1].(primitive.Binary).Data)
						copy(newPayload[len(serverPayloads[len(serverPayloads)-1].(primitive.Binary).Data):], payload)
						streamUpdate[fmt.Sprintf("serverpayloads.%d", len(serverPayloads)-1)] = newPayload
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
				FirstTime:      timestamp,
				LastTime:       timestamp,
				Terminated:     false,
				ClientIp:       clientIp,
				ServerIp:       serverIp,
				ClientPort:     clientPort,
				ServerPort:     serverPort,
				Protocol:       protocol,
				BaseSeq:        packet["tcpseq"].(int64),
				ClientPayloads: [][]byte{},
				ServerPayloads: [][]byte{},
			}
			if fromClient {
				stream.ClientPayloads = append(stream.ClientPayloads, payload)
			} else {
				stream.ServerPayloads = append(stream.ServerPayloads, payload)
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
