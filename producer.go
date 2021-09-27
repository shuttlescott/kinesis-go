package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load() //Load .env file
	if err != nil {
		log.Panic(err)
	}

	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(os.Getenv("KINESIS_REGION")))
	if err != nil {
		log.Panic(err)
	}
	client := kinesis.NewFromConfig(cfg)

	streamName := aws.String(os.Getenv("KINESIS_STREAM_NAME"))
	_, err = client.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Panic(err)
	}

	data := openFile()
	putOutput, err := client.PutRecord(ctx, &kinesis.PutRecordInput{
		Data:         []byte(data),
		StreamName:   streamName,
		PartitionKey: aws.String("key1"),
	})
	if err != nil {
		panic(err)
	}
	log.Printf("%v\n", *putOutput)
}

func openFile() string {
	jsonFile, err := os.Open("data.json")

	if err != nil {
		log.Panic(err)
	}
	log.Println("Successfully Opened data.json")
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	return string(byteValue)
}
