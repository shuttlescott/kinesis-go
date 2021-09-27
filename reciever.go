package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

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
	streams, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Panic(err)
	}

	// retrieve iterator
	iteratorOutput, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: "TRIM_HORIZON",
		StreamName:        streamName,
	})
	if err != nil {
		log.Panic(err)
	}

	shardIterator := iteratorOutput.ShardIterator
	var a *string

	for {
		records, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// process the data
		if len(records.Records) > 0 {
			for _, d := range records.Records {
				m := make(map[string]interface{})
				err := json.Unmarshal([]byte(d.Data), &m)
				if err != nil {
					log.Println(err)
					continue
				}
				log.Printf("GetRecords Data: %v\n", m)
			}
		} else if records.NextShardIterator == a || shardIterator == records.NextShardIterator || err != nil {
			log.Printf("GetRecords ERROR: %v\n", err)
			break
		}
		shardIterator = records.NextShardIterator
		time.Sleep(1000 * time.Millisecond)
	}
}
