## Testing producing and receiving events via AWS Kinesis stream

Produce events by defining data in [data.json](data.json) then running
```bash
go run producer.go
```

Receive events by running 
```bash
go run reciever.go
```
