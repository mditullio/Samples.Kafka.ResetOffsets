using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Tools.ProduceMessages;


var bootstrapServers = "localhost:9092";
var topicName = "va.poc.contacts.v1";

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.All,
    CompressionType = CompressionType.None
};

using var producer = new ProducerBuilder<string, Contact>(producerConfig)
    .SetValueSerializer(new ContactSerializer())
    .Build();

var numberOfMessages = 50;

Console.WriteLine($"Producing {numberOfMessages} messages to topic: {topicName}");

var baseDate = new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc);

for (int i = 1; i <= numberOfMessages; i++)
{
    var contact = new Contact
    (
        $"CONTACT-{i}",
        $"Contact {i}",
        $"contact{i}@example.com",
        $"077-456-{i}",
        baseDate.AddDays(i)
    );

    producer.Produce(topicName, new Message<string, Contact>
    {
        Key = contact.Id.ToString(),
        Value = contact,
        Timestamp = new Timestamp(contact.UpdatedAt!.Value, TimestampType.CreateTime)
    });

    Console.WriteLine($"Produced message {i} to topic: {topicName}");
}

producer.Flush();

Console.WriteLine("All messages have been produced.");
