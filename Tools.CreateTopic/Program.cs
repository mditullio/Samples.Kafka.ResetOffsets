using Confluent.Kafka;
using Confluent.Kafka.Admin;

var bootstrapServers = "localhost:9092";
var topicName = "va.poc.contacts.v1";
var adminClientConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
using var adminClient = new AdminClientBuilder(adminClientConfig).Build();

Console.WriteLine($"Processing Topic: {topicName}");

try
{
    await adminClient.CreateTopicsAsync(
        [
            new TopicSpecification {
                Name = topicName,
                ReplicationFactor = 1,
                NumPartitions = 2,
                Configs = new Dictionary<string, string>
                {
                    ["cleanup.policy"] = "compact",
                    ["message.timestamp.type"] = "CreateTime",
                    ["max.compaction.lag.ms"] = "1000"
                }
            }
        ]);

    var createdTopic = await adminClient.DescribeConfigsAsync(
        [
            new ConfigResource
            {
                Type = ResourceType.Topic,
                Name = topicName
            }
        ]);
    
    Console.WriteLine($"Topic {topicName} created successfully.");

    foreach (var config in createdTopic[0].Entries)
    {
        var configResult = config.Value;
        Console.WriteLine("  " + configResult.Name + " = " + configResult.Value);
    }
}
catch (CreateTopicsException e) when (e.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
{
    Console.WriteLine($"Topic {topicName} already exists.");
}

