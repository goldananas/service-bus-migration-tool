namespace ServiceBusCli.Models;

public class ServiceBusEndpoint
{
    public string ConnectionString { get; set; } = string.Empty;
    public string QueueName { get; set; } = string.Empty;
    public bool UseDlq { get; set; } = true;
}
