namespace Tools.ProduceMessages;

public record Contact(
    string Id,
    string Name,
    string Email,
    string PhoneNumber,
    DateTime? UpdatedAt);
