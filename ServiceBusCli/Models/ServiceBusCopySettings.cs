namespace ServiceBusCli.Models;

public class ServiceBusCopySettings
{
    public ServiceBusEndpoint Source { get; set; } = new();
    public ServiceBusEndpoint Destination { get; set; } = new();
}
