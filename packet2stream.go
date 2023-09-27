package main

import (
	"context"
	"flag"
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
var streamTimeout = flag.Int64("t", 60*1000000000, "Stream timeout in seconds")

var ctxTodo = context.TODO()

var database *mongo.Database
var packetCol *mongo.Collection
var streamCol *mongo.Collection

// Stream struct
type Stream struct {
	FirstTime  int64
	LastTime   int64
	Terminated bool
	ClientIp   string
	ServerIp   string
	ClientPort int64
	ServerPort int64
	Protocol   string
	BaseSeq    int64
	Payload    []byte
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

		// Check direction of packet - the simple way to do this is compare the port numbers (if they're equal, arbitrarily choose the lower IP address to avoid collisions)
		if packet["srcport"].(int64) < packet["dstport"].(int64) || packet["srcport"].(int64) == packet["dstport"].(int64) && packet["srcip"].(string) < packet["dstip"].(string) {
			clientIp = packet["dstip"].(string)
			serverIp = packet["srcip"].(string)
			clientPort = packet["dstport"].(int64)
			serverPort = packet["srcport"].(int64)
		} else {
			clientIp = packet["srcip"].(string)
			serverIp = packet["dstip"].(string)
			clientPort = packet["srcport"].(int64)
			serverPort = packet["dstport"].(int64)
		}

		packetId := packet["_id"].(primitive.ObjectID)
		timestamp := packet["timestamp"].(int64)
		protocol := packet["protocol"].(string)
		tcpflags := packet["tcpflag"].(bson.M)
		payload := packet["payload"].(primitive.Binary)

		// Read payload as []byte
		payloadData := payload.Data

		streamCursor, err := streamCol.Find(ctxTodo, bson.M{"terminated": false, "clientip": clientIp, "serverip": serverIp, "clientport": clientPort, "serverport": serverPort, "protocol": protocol, "lasttime": bson.M{"$lt": timestamp + *streamTimeout}})

		// If the packet matches an existing stream, update the stream
		if err == nil {
			var stream bson.M = make(map[string]interface{})
			streamCursor.Next(ctxTodo)
			err = streamCursor.Decode(&stream)

			// Check if stream exists
			if err == mongo.ErrNoDocuments || stream["_id"] == nil {
				// Create a new stream
				stream := Stream{
					FirstTime:  timestamp,
					LastTime:   timestamp,
					Terminated: false,
					ClientIp:   clientIp,
					ServerIp:   serverIp,
					ClientPort: clientPort,
					ServerPort: serverPort,
					Protocol:   protocol,
					BaseSeq:    packet["tcpseq"].(int64),
					Payload:    payloadData,
				}
				createStream(stream, packetId)
				continue
			}

			streamUpdate := bson.M{}
			streamId := stream["_id"].(primitive.ObjectID)

			if protocol == "tcp" {
				if tcpflags["fin"] == true || tcpflags["rst"] == true {
					streamUpdate["terminated"] = true
				}
			}

			// Append the payload to the stream
			if len(payloadData) > 0 {
				streamPayload := stream["payload"].(primitive.Binary)
				streamUpdate["payload"] = append(streamPayload.Data, payloadData...)
			}

			// Update the stream
			_, err = streamCol.UpdateOne(ctxTodo, bson.M{"_id": streamId}, bson.M{"$set": streamUpdate})
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
				FirstTime:  timestamp,
				LastTime:   timestamp,
				Terminated: false,
				ClientIp:   clientIp,
				ServerIp:   serverIp,
				ClientPort: clientPort,
				ServerPort: serverPort,
				Protocol:   protocol,
				BaseSeq:    packet["tcpseq"].(int64),
				Payload:    payloadData,
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
