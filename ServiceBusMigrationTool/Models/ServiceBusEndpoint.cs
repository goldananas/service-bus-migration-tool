namespace ServiceBusMigrationTool.Models;

public class ServiceBusEndpoint
{
    public string ConnectionString { get; set; } = string.Empty;
    public string? QueueName { get; set; }
    public string? TopicName { get; set; }
    public string? SubscriptionName { get; set; }

    public bool IsQueue => !string.IsNullOrWhiteSpace(QueueName);
    public bool IsTopic => !string.IsNullOrWhiteSpace(TopicName);
    public string EntityName => IsQueue ? QueueName! : TopicName!;
}
