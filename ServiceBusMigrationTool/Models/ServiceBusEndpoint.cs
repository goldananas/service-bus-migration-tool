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

    /// <summary>
    /// Extracts the hostname from the connection string for safe logging (no keys).
    /// </summary>
    public string HostName
    {
        get
        {
            // Parse the Endpoint value without embedding a literal connection string pattern
            const string prefix = "sb://";
            var idx = ConnectionString.IndexOf(prefix, StringComparison.OrdinalIgnoreCase);
            if (idx < 0) return "(unknown)";
            var start = idx + prefix.Length;
            var end = ConnectionString.IndexOfAny(new[] { ';', '/' }, start);
            return end > start ? ConnectionString[start..end] : ConnectionString[start..];
        }
    }
}
