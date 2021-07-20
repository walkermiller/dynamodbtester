package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
var action, table, primaryKey, sortKey string
var batch, logswitch bool
var startTime time.Time

var results []string

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
	flag.StringVar(&primaryKey, "primaryKey", LookupEnvOrString("PRIMARYKEY", "AccountId"), "Primary Key to use. Defaults to ResourceId")
	flag.StringVar(&sortKey, "sortKey", LookupEnvOrString("SORTKEY", "ResourceId"), "Primary Key to use. Defaults to AccountId")
	flag.BoolVar(&batch, "batch", LookupEnvOrBool("BATCH", false), "batch requests up or not. Default is false")
	flag.BoolVar(&logswitch, "log", LookupEnvOrBool("LOG", false), "log results or not. Default is false")
	flag.Parse()

	startTime = time.Now()

	if action == "query" {
		messagestoprocess = 1
	}

	totalMessages := messagestoprocess * threads

	if logswitch == false {
		p, _ = pterm.DefaultProgressbar.WithTotal(totalMessages).WithTitle(fmt.Sprintf("Processing %d Messages", totalMessages)).Start()
	}

	var wg sync.WaitGroup

	for i := 1; i < threads+1; i++ {
		wg.Add(1)
		switch action {
		case "put":
			go threadPut(messagestoprocess, &wg, i)
		case "get":
			go threadGet(messagestoprocess, &wg, i)
		case "query":
			go threadQuery(&wg, i)
		}

	}

	wg.Wait()

	if logswitch {
		print(strings.Join(results, "\n"))
	}

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

func LookupEnvOrBool(key string, defaultVal bool) bool {
	if _, ok := os.LookupEnv(key); ok {
		return true
	}
	return defaultVal
}

func putItems(startPosition int, thread int) {

	var items []*dynamodb.WriteRequest

	for i := startPosition; i < startPosition+25; i++ {

		request := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					primaryKey: {
						N: aws.String(strconv.Itoa(thread)),
					},
					sortKey: {
						N: aws.String(strconv.Itoa(i)),
					},
				},
			},
		}

		items = append(items, request)
		messages++
		// =log.Printf("Thread: %d, Number: %d", thread, i)
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			table: items,
		},
	}

	result, err := svc.BatchWriteItem(input)
	if err != nil {
		log.Fatalf("Thread %d Got error calling BatchWriteItem: %s", thread, err)
	}
	logResult(result.String())
	// log.Printf("Thread %d finished %d", thread, finalPosition)

}

func getItem(primaryKeyValue, sortKeyValue string) {
	// defer p.Increment()
	// log.Printf("Getting item from %s with primary key %s = %s", table, primaryKey, resourceId)
	input := &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			primaryKey: {
				N: aws.String(primaryKeyValue),
			},
			sortKey: {
				N: aws.String(sortKeyValue),
			},
		},
	}
	// println(input.Key)
	result, err := svc.GetItem(input)
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	if result.Item == nil {
		msg, _ := fmt.Printf("Could not find primaryKey %s with sortKey %s", primaryKeyValue, sortKeyValue)
		log.Print(msg)
	}

	// err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	// if err != nil {
	// 	panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	// }

	logResult(result.String())
}

func batchGetItem(startPosition, thread int) {

	var items []map[string]*dynamodb.AttributeValue

	for i := startPosition; i < startPosition+100; i++ {

		newEntry := map[string]*dynamodb.AttributeValue{
			primaryKey: {
				N: aws.String(strconv.Itoa(thread)),
			},
			sortKey: {
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

	result, err := svc.BatchGetItem(input)
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
	logResult(result.String())

}

func query(primaryKeyValue int) {
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				N: aws.String(strconv.Itoa(primaryKeyValue)),
			},
		},
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :v1", primaryKey)),
		TableName:              aws.String(table),
	}

	result, err := svc.Query(input)
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

	logResult(result.String())
}

func threadPut(m int, wg *sync.WaitGroup, thread int) {
	defer incrementPB(m)
	defer wg.Done()

	for i := 1; i < m+1; i += 25 {
		putItems(((thread-1)*m)+i, thread)
	}

}

func threadGet(m int, wg *sync.WaitGroup, thread int) {
	defer wg.Done()
	defer incrementPB(m)
	for i := 1; i < m+1; i += 1 {
		switch batch {
		case true:
			batchGetItem((thread-1)*m+i, thread)
			i += 99
		case false:
			getItem(strconv.Itoa(thread), strconv.Itoa(((thread-1)*m)+1))
		}
	}
}

func threadQuery(wg *sync.WaitGroup, thread int) {
	defer wg.Done()
	defer incrementPB(1)
	query(thread)

}

func logResult(result string) {
	if logswitch == true {
		log.Println(result)
	}
}

func incrementPB(i int) {
	if logswitch == false {
		p.Add(i)
	}
}
