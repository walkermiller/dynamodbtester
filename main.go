package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"golang.org/x/net/http2"
)

type Entry struct {
	ResourceId int
	AccountId  int
}

type HTTPClientSettings struct {
	Connect          time.Duration
	ConnKeepAlive    time.Duration
	ExpectContinue   time.Duration
	IdleConn         time.Duration
	MaxAllIdleConns  int
	MaxHostIdleConns int
	ResponseHeader   time.Duration
	TLSHandshake     time.Duration
}

var svc *dynamodb.DynamoDB

var messages, messagestoprocess, threads int
var action, table, primaryKey, sortKey string
var batch, logswitch bool
var wg sync.WaitGroup

// var results chan string

// var p *pterm.ProgressbarPrinter

func main() {

	// results = make(chan string)
	httpClient, _ := NewHTTPClientWithSettings(HTTPClientSettings{
		Connect:          5 * time.Second,
		ExpectContinue:   1 * time.Second,
		IdleConn:         0,
		ConnKeepAlive:    30 * time.Second,
		MaxAllIdleConns:  0,
		MaxHostIdleConns: 4000,
		ResponseHeader:   5 * time.Second,
		TLSHandshake:     5 * time.Second,
	})

	svc = dynamodb.New(session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			HTTPClient: httpClient,
		},
		SharedConfigState: session.SharedConfigEnable,
	})))

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

	// This initializes the svc to begin with.
	_, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	dydbError(err)

	totalMessages := messagestoprocess * threads

	title := fmt.Sprintf("%s test of %d threads and %d Messages (total: %d)", strings.Title(action), threads, messagestoprocess, totalMessages)
	fmt.Println(title)

	// p, _ = pterm.DefaultProgressbar.WithTotal(totalMessages).WithTitle(title).Start()

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
			// query(i)
			go threadQuery(i)
		}

	}
	// for result := range results {
	// 	log.Println(result)
	// }
	// close(results)
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
	// log.Println(items)
	_, err := svc.BatchWriteItem(input)
	dydbError(err)
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
	dydbError(err)
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
	dydbError(err)
	// logResult(result.String())

}

func query(primaryKeyValue int) {

	input := &dynamodb.QueryInput{

		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				N: aws.String(strconv.Itoa(primaryKeyValue)),
			},
		},
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :v1", primaryKey)),
		TableName:              aws.String(table),
	}

	beginTime := time.Now()
	response, err := svc.Query(input)
	log.Printf("Key: %d, Count: %d, Capacity Consumed: %f, Reponse Time: %dms",
		primaryKeyValue,
		*response.Count,
		*response.ConsumedCapacity.CapacityUnits, time.Since(beginTime).Milliseconds())

	dydbError(err)
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
	defer wg.Done()
	wg.Add(1)
	// startTime := time.Now()
	query(thread)
	// incrementPB(1)
}

// func incrementPB(i int) {
// 	if !logswitch {
// 		p.Add(i)
// 	}
// }

// func recordResponseTime(message string, startTime time.Time, total int) {
// 	dur := time.Since(startTime).Seconds()
// 	log.Printf("%s: %fs. Avg Response: %fs", message, dur, (float64(total) / dur))
// }

func NewHTTPClientWithSettings(httpSettings HTTPClientSettings) (*http.Client, error) {
	var client http.Client
	tr := &http.Transport{
		ResponseHeaderTimeout: httpSettings.ResponseHeader,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: httpSettings.ConnKeepAlive,
			DualStack: true,
			Timeout:   httpSettings.Connect,
		}).DialContext,
		MaxIdleConns:          httpSettings.MaxAllIdleConns,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       httpSettings.IdleConn,
		TLSHandshakeTimeout:   httpSettings.TLSHandshake,
		MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
		ExpectContinueTimeout: httpSettings.ExpectContinue,
	}

	// So client makes HTTP/2 requests
	err := http2.ConfigureTransport(tr)
	if err != nil {
		return &client, err
	}

	return &http.Client{
		Transport: tr,
	}, nil
}

func dydbError(err error) {
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
