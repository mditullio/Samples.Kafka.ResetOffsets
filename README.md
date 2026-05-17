# Samples.Kafka.ResetOffsets

A proof of concept demonstrating how to reset a Kafka consumer group's offsets to a specific point in time — without admin rights on the topic — using the consumer API itself.

## The use cases

Resetting consumer offsets is useful whenever you need to intentionally replay data without changing the topic itself. Typical cases include:

- Reprocessing messages after fixing a bug in the consumer or downstream pipeline.
- Replaying a time window after a deployment, incident, or temporary outage.
- Backfilling a new consumer when it needs to start from a specific point in history instead of from the tail.
- Migrating a data product from topic `v1` to topic `v2` while keeping the same logical stream.
- Running a one-off validation or reconciliation job against an older slice of the stream.
- Recovering from partial processing failures where some messages were committed but not fully handled.

In short, the pattern is a good fit when the data is still valid, but the consumer needs to see it again from a controlled timestamp.


## What this PoC shows

Four console tools, meant to be run in order:

| Project | What it does |
|---|---|
| `Tools.CreateTopic` | Creates the compacted topic with explicit config |
| `Tools.ProduceMessages` | Produces 50 `Contact` messages with a **client-supplied timestamp** |
| `Tools.ConsumeMessages` | Consumes messages from the topic, committing offsets manually |
| `Tools.ResetOffsets` | Resets the consumer group's committed offsets to a given timestamp |

## Preparing the environment

If you don't have a local Kafka environment, you can set up one by using the `docker-compose.yml` file present in the solution.

This will spawn a small Kafka cluster, with a broker, a schema registry and Redpanda UI console.

## Running the tools

```bash
# 1. Create the topic
dotnet run --project Tools.CreateTopic

# 2. Produce messages
dotnet run --project Tools.ProduceMessages

# 3. Consume messages (Ctrl+C to stop)
dotnet run --project Tools.ConsumeMessages

# 4. Reset offsets to a specific timestamp, then re-run the consumer
dotnet run --project Tools.ResetOffsets
dotnet run --project Tools.ConsumeMessages
```

## Topic configuration

The topic `va.poc.contacts.v1` is created with:

- `cleanup.policy = compact` — retains only the latest message per key
- `message.timestamp.type = CreateTime` — the broker preserves the producer-supplied timestamp
- `max.compaction.lag.ms = 1000` — compaction runs as soon as possible after the lag window

## Offset reset mechanics

`Tools.ResetOffsets` resets offsets without admin rights by acting as a consumer in the same group:

1. Fetches partition metadata via a `DependentAdminClientBuilder` (plain metadata request, no ACLs needed).
2. Calls `consumer.OffsetsForTimes(...)` to resolve each partition's target offset.
3. If the timestamp is beyond the last message in a partition, `OffsetsForTimes` returns `Offset.End` — the consumer will resume at the tail, picking up only future messages.
4. Calls `consumer.Assign(...)` and `consumer.Commit(...)` to get partition assignment and commit the offsets to the group.

> The group must be inactive (no running consumer) when the reset is applied — Kafka rejects the commit with `NonEmptyGroup` otherwise.

## Why client-supplied timestamps matter for offset reset

Kafka's `OffsetsForTimes` is the only **broker-indexed** lookup available to clients. For a given timestamp it returns, per partition, the earliest offset whose record timestamp is >= that value — this is a binary search on the broker, constant-time regardless of partition size.

Every other message attribute — key, headers, payload content — has no broker-side index. Finding a specific value by those means requires scanning messages forward from a known offset, which is impractical for large partitions.

This means **timestamp is the only affordable metadata to seek by**.

### Making offset resets idempotent with client-supplied timestamps

In this code example, the producer sets a fixed, deterministic timestamp on each message instead of relying on `DateTime.UtcNow`:

```csharp
producer.Produce(topicName, new Message<string, Contact>
{
    Key = contact.Id,
    Value = contact,
    Timestamp = new Timestamp(new DateTime(2026, 5, 1, 12, 0, 0, DateTimeKind.Utc))
});
```

Because the timestamps are known in advance and stable across runs, `Tools.ResetOffsets` can be called multiple times with the same argument and will always resolve to the same offsets. The reset is idempotent: re-running the consumer after a reset always replays the same set of messages, regardless of when the tools are executed.

If the producer used `DateTime.UtcNow` instead, the resolved offsets would shift every run, making the behavior non-reproducible.

### CreateTime vs LogAppendTime

This works because the topic uses `message.timestamp.type = CreateTime`, which is the default configuration for Kafka topics. 

With `LogAppendTime` the broker overwrites the producer's timestamp with its own ingestion time, making client-supplied timestamps irrelevant. The lookup still works, but you lose control over reproducibility.

