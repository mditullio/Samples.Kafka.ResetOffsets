using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace Tools.ProduceMessages;

public class ContactSerializer : ISerializer<Contact>
{
    public byte[] Serialize(Contact data, SerializationContext context)
    {
        return JsonSerializer.SerializeToUtf8Bytes(data);
    }
}
