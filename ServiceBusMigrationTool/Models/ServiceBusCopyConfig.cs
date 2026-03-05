namespace ServiceBusMigrationTool.Models;

public class ServiceBusCopyConfig
{
    public List<ServiceBusCopyOperation> Operations { get; set; } = new();
}
