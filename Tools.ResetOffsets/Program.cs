using Confluent.Kafka;

var bootstrapServers = "localhost:9092";
var topicName = "va.poc.contacts.v1";
var groupId = "tools-consume-messages-group";

var targetTimestamp = new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(45);
var requestTimeout = TimeSpan.FromSeconds(30);

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    EnableAutoCommit = false,
    // We won't poll — we only need a consumer attached to the group
    // to call OffsetsForTimes and Commit. AutoOffsetReset is irrelevant here.
};

using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
using var adminClient = new DependentAdminClientBuilder(consumer.Handle).Build();

Console.WriteLine($"Topic     : {topicName}");
Console.WriteLine($"Group     : {groupId}");
Console.WriteLine($"Timestamp : {targetTimestamp:O}");
Console.WriteLine();

// 1. Discover partitions via metadata (plain wire request — no admin ACLs needed).
var metadata = adminClient.GetMetadata(topicName, requestTimeout);
var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topicName)
    ?? throw new InvalidOperationException($"Topic '{topicName}' not found.");

if (topicMetadata.Error.IsError)
    throw new InvalidOperationException($"Metadata error for '{topicName}': {topicMetadata.Error.Reason}");


var partitions = topicMetadata.Partitions
    .Select(p => new TopicPartition(topicName, new Partition(p.PartitionId)))
    .ToList();

Console.WriteLine($"Found {partitions.Count} partition(s): {string.Join(", ", partitions.Select(p => p.Partition.Value))}");

// 2. Resolve offset-for-timestamp per partition.
var query = partitions
    .Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(targetTimestamp, TimestampType.CreateTime)))
    .ToList();

var resolved = consumer.OffsetsForTimes(query, requestTimeout);

// 3. For partitions where no message has timestamp >= target (Offset.Unset),
//    fall back to the partition's high watermark so the consumer resumes at the tail.
var toCommit = new List<TopicPartitionOffset>(resolved.Count);
foreach (var tpo in resolved)
{
    Console.WriteLine($"  P{tpo.Partition.Value}: offset {tpo.Offset.Value}");
    toCommit.Add(tpo);
}

// 4. Commit the offsets to the consumer group.
// Note: this will fail if a live consumer in the same group currently owns these partitions.
Console.WriteLine();
Console.WriteLine("Committing offsets...");
consumer.Assign(toCommit);
consumer.Commit(toCommit);

Console.WriteLine("Done. Next run of the consumer in this group will start from the committed offsets.");
