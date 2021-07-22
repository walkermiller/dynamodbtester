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

var responseTimes []string
var results []string

var p *pterm.ProgressbarPrinter

func main() {
	defer recordResponseTime("Total time taken", time.Now())
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

	if action == "query" {
		messagestoprocess = 1
	}

	totalMessages := messagestoprocess * threads

	title := fmt.Sprintf("%s test of %d threads and %d Messages (total: %d)", strings.Title(action), threads, messagestoprocess, totalMessages)
	fmt.Println(title)

	// p, _ = pterm.DefaultProgressbar.WithTotal(totalMessages).WithTitle(title).Start()

	var wg sync.WaitGroup

	for i := 1; i < threads+1; i++ {
		// wg.Add(1)
		switch action {
		case "put":
			wg.Add(1)
			go threadPut(messagestoprocess, &wg, i)
		case "get":
			wg.Add(1)
			go threadGet(messagestoprocess, &wg, i)
		case "query":
			threadQuery(i)
		}

	}

	wg.Wait()

	// print(strings.Join(responseTimes, "\n"))

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
	if val, ok := os.LookupEnv(key); ok {
		if val == "True" {
			return true
		}
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
						S: aws.String(strconv.Itoa(thread)),
					},
					sortKey: {
						S: aws.String(strconv.Itoa(i)),
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

	_, err := svc.BatchWriteItem(input)
	if err != nil {
		log.Fatalf("Thread %d Got error calling BatchWriteItem: %s", thread, err)
	}
	// logResult(result.String())
	// log.Printf("Thread %d finished %d", thread, finalPosition)

}

func getItem(primaryKeyValue, sortKeyValue string) {
	// defer p.Increment()
	// log.Printf("Getting item from %s with primary key %s = %s", table, primaryKey, resourceId)
	input := &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			primaryKey: {
				S: aws.String(primaryKeyValue),
			},
			sortKey: {
				S: aws.String(sortKeyValue),
			},
		},
	}
	// println(input.Key)
	_, err := svc.GetItem(input)
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	// if result.Item == nil {
	// 	msg, _ := fmt.Printf("Could not find primaryKey %s with sortKey %s", primaryKeyValue, sortKeyValue)
	// 	log.Print(msg)
	// }

	// err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	// if err != nil {
	// 	panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	// }

	// logResult(result.String())
}

func batchGetItem(startPosition, thread int) {

	var items []map[string]*dynamodb.AttributeValue

	for i := startPosition; i < startPosition+100; i++ {

		newEntry := map[string]*dynamodb.AttributeValue{
			primaryKey: {
				S: aws.String(strconv.Itoa(thread)),
			},
			sortKey: {
				S: aws.String(strconv.Itoa(i)),
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
	// logResult(result.String())

}

func query(primaryKeyValue int) {
	defer recordResponseTime(fmt.Sprintf("Query response time for key %d", primaryKeyValue), time.Now())
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(strconv.Itoa(primaryKeyValue)),
			},
		},
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :v1", primaryKey)),
		TableName:              aws.String(table),
	}
	_, err := svc.Query(input)

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

	// logResult(result.String())
}

func threadPut(m int, wg *sync.WaitGroup, thread int) {
	defer wg.Done()

	for i := 1; i < m+1; i += 25 {
		putItems(((thread-1)*m)+i, thread)
	}
	// incrementPB(m)

}

func threadGet(m int, wg *sync.WaitGroup, thread int) {
	defer wg.Done()
	for i := 1; i < m+1; i += 1 {
		switch batch {
		case true:
			batchGetItem((thread-1)*m+i, thread)
			i += 99
		case false:
			getItem(strconv.Itoa(thread), strconv.Itoa(((thread-1)*m)+1))
		}
	}
	// incrementPB(m)
}

func threadQuery(thread int) {
	// defer wg.Done()
	// startTime := time.Now()
	query(thread)
	// incrementPB(1)
}

func logResult(result string) {
	if logswitch {
		log.Println(result)
	}
}

func incrementPB(i int) {
	if !logswitch {
		p.Add(i)
	}
}

func recordResponseTime(message string, startTime time.Time) {
	log.Printf("%s: %dms", message, time.Since(startTime).Milliseconds())
}
