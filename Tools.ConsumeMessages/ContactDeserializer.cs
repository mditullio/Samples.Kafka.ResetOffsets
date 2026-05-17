using Confluent.Kafka;
using System.Text.Json;

namespace Tools.ConsumeMessages;

public class ContactDeserializer : IDeserializer<Contact>
{
    public Contact Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty)
            throw new InvalidOperationException("Cannot deserialize null or empty data.");

        return JsonSerializer.Deserialize<Contact>(data)
            ?? throw new InvalidOperationException("Deserialized Contact was null.");
    }
}
