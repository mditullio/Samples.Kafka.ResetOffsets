using Confluent.Kafka;
using Tools.ConsumeMessages;

var bootstrapServers = "localhost:9092";
var topicName = "va.poc.contacts.v1";
var groupId = "tools-consume-messages-group";

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<string, Contact>(consumerConfig)
    .SetValueDeserializer(new ContactDeserializer())
    .Build();

consumer.Subscribe(topicName);

Console.WriteLine($"Consuming messages from topic: {topicName} (group: {groupId})");
Console.WriteLine("Press Ctrl+C to stop.");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var result = consumer.Consume(cts.Token);

        var contact = result.Message.Value;
        Console.WriteLine(
            $"[{result.TopicPartitionOffset}] Key={result.Message.Key} " +
            $"| Id={contact.Id} Name={contact.Name} Email={contact.Email} " +
            $"Phone={contact.PhoneNumber} UpdatedAt={contact.UpdatedAt:O}");

        consumer.Commit(result);
    }
}
catch (OperationCanceledException)
{
    // normal shutdown
}
finally
{
    consumer.Close();
    Console.WriteLine("Consumer closed.");
}
