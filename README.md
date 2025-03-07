## sample redplanda producer

This code contains a sample application showcasing an issue [#937](https://github.com/twmb/franz-go/issues/937) in franz-go.

## Reproducing the issue

The application is designed to run indefinitely (until SIGINT is received) and produce batches of records to a Redpanda cluster in multiple
goroutines concurrently.

### No retries

1. Start the local cluster: `docker compose up -d`.
   It will create a cluster and a `test-topic` topic with 60 partitions and 3 replicas.
2. Run the producer: `go run ./... -tries=1`
   By default it will run 10 publishing workers sending batches each second (with some jitter). Without retries.
3. Use `rpk` to put one of the brokers into maintenance mode, there's a make target for that: `make start-maintenance`.
   It puts broker 1 into maintenance mode.
4. Observe logs from the running application, once the broker starts draining its partitions, the application will start
   to log errors about not being able to produce messages with NOT_LEADER_FOR_PARTITION error.

`NOT_LEADER_FOR_PARTITION` responses will continue up until `MetadataMaxAge` is reached and the producer refreshes the metadata. This can be 
confirmed by changing the `MetadataMaxAge` (with `-metadata-max-age` flag) to a lower value and observing the logs.

### With retries

Repeat steps 2-4 but run the producer with `-tries=0` flag. This will make the producer retry records, but most importantly forcefully update
the metadata on the first occurence of `NOT_LEADER_FOR_PARTITION` error.

## Expected behavior

The producer should be able to recover from `NOT_LEADER_FOR_PARTITION` by refreshing the metadata irrespective of the number of retries 
set on the client.
