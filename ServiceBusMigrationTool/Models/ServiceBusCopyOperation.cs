namespace ServiceBusMigrationTool.Models;

public class ServiceBusCopyOperation
{
    public int BatchSize { get; set; } = 100;
    public bool DryRun { get; set; }
    public bool DlqOnly { get; set; }
    public ServiceBusEndpoint Source { get; set; } = new();
    public ServiceBusEndpoint Destination { get; set; } = new();
}
