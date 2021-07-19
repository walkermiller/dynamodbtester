package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pterm/pterm"
)

type Entry struct {
	ResourceId int
	AccountId  int
}

var svc *dynamodb.DynamoDB = dynamodb.New(session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
})))

var messages, messagestoprocess, threads int
var action, table, primaryKey string
var batch bool
var startTime time.Time

var p *pterm.ProgressbarPrinter

func main() {
	// mySession, _ := session.NewSession(&aws.Config{
	// 	EnableEndpointDiscovery: aws.Bool(true),
	// })
	// svc = dynamodb.New(mySession)

	flag.IntVar(&messagestoprocess, "messages", LookupEnvOrInt("MESSAGES", 100), "messages to run per thread")
	flag.IntVar(&threads, "threads", LookupEnvOrInt("THREADS", 10), "number of threads to run")
	flag.StringVar(&action, "action", LookupEnvOrString("ACTION", "put"), "specify put or get. Defaults to put")
	flag.StringVar(&table, "table", LookupEnvOrString("TABLE", "Test"), "Table name to use. Defaults to Test")
	flag.StringVar(&primaryKey, "primaryKey", LookupEnvOrString("PRIMARYKEY", "ResourceId"), "Primary Key to use. Defaults to ResourceId")
	flag.BoolVar(&batch, "batch", false, "batch requests up or not. Default is false")
	flag.Parse()

	thread := 1
	startTime = time.Now()

	totalMessages := messagestoprocess * threads

	p, _ = pterm.DefaultProgressbar.WithTotal(totalMessages).WithTitle(fmt.Sprintf("Processing %d Messages", totalMessages)).Start()

	var wg sync.WaitGroup
	// log.Printf("Running for %d seconds", desiredduration)

	for i := 1; i < threads+1; i++ {
		wg.Add(1)
		switch action {
		case "put":
			go threadPut(messagestoprocess, &wg, thread)
		case "get":
			go threadGet(messagestoprocess, &wg, thread)
			thread++
		}

	}

	wg.Wait()

}

func LookupEnvOrString(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func LookupEnvOrInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		v, err := strconv.Atoi(val)
		if err != nil {
			log.Fatalf("LookupEnvOrInt[%s]: %v", key, err)
		}
		return v
	}
	return defaultVal
}

func putItems(startPosition int, thread int) {

	var items []*dynamodb.WriteRequest

	for i := startPosition; i < startPosition+25; i++ {

		// av, err := dynamodbattribute.MarshalMap(Entry{
		// 	ResourceId: i,
		// 	AccountId:  i,
		// })
		// if err != nil {
		// 	log.Fatalf("Thread %d got error when Marshalling: %s", thread, err)
		// }
		request := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					primaryKey: {
						N: aws.String(strconv.Itoa(i)),
					},
				},
			},
		}

		items = append(items, request)
		messages++

	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			table: items,
		},
	}

	_, err := svc.BatchWriteItem(input)
	if err != nil {
		log.Fatalf("Thread %d Got error calling BatchWriteItem: %s", thread, err)
	}

	// log.Printf("Thread %d finished %d", thread, finalPosition)

}

func getItem(resourceId string) {

	// log.Printf("Getting item from %s with primary key %s = %s", table, primaryKey, resourceId)
	input := &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			primaryKey: {
				N: aws.String(resourceId),
			},
		},
	}
	// println(input.Key)
	result, err := svc.GetItem(input)
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	if result.Item == nil {
		msg := "Could not find '" + resourceId + "'"
		log.Print(msg)
	}

	item := Entry{}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

}

func batchGetItem(startPosition int) {

	var items []map[string]*dynamodb.AttributeValue

	for i := startPosition; i < startPosition+25; i++ {

		newEntry := map[string]*dynamodb.AttributeValue{
			primaryKey: {
				N: aws.String(strconv.Itoa(i)),
			},
		}
		items = append(items, newEntry)

	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			table: {
				Keys: items,
			},
		},
	}

	_, err := svc.BatchGetItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}
}

func threadPut(m int, wg *sync.WaitGroup, thread int) {
	// defer p.Add(m)
	defer wg.Done()

	for i := 1; i < m+1; i += 25 {
		putItems((thread-1)*m+1, thread)
	}

}

func threadGet(m int, wg *sync.WaitGroup, thread int) {
	defer wg.Done()
	defer p.Add(m)
	for i := 1; i < m+1; i += 1 {
		switch batch {
		case true:
			batchGetItem((thread-1)*m + 1)
		case false:
			getItem(strconv.Itoa((thread-1)*m + 1))
			i += 24
		}
	}
}
