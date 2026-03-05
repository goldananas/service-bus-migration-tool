namespace ServiceBusMigrationTool.Models;

public class NamespaceMigrationConfig
{
    public string SourceConnectionString { get; set; } = string.Empty;
    public string DestinationConnectionString { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 100;
    public int MaxConcurrency { get; set; } = 5;
    public bool DryRun { get; set; }
}
