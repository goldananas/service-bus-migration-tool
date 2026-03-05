using System.Text.Json.Serialization;

namespace ServiceBusMigrationTool.Models;

public class MigrationState
{
    public DateTime StartedAt { get; set; }
    public DateTime LastUpdatedAt { get; set; }
    public string Mode { get; set; } = string.Empty;
    public Dictionary<string, EntityMigrationState> Entities { get; set; } = new();
}

public class EntityMigrationState
{
    public string EntityKey { get; set; } = string.Empty;
    public string DestinationEntity { get; set; } = string.Empty;
    public SubQueueState MainQueue { get; set; } = new();
    public SubQueueState DeadLetterQueue { get; set; } = new();

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public EntityStatus Status { get; set; } = EntityStatus.Pending;

    public int TotalMessagesCopied { get; set; }
}

public class SubQueueState
{
    public long LastSuccessfulSequenceNumber { get; set; }
    public int MessagesCopied { get; set; }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public SubQueueStatus Status { get; set; } = SubQueueStatus.Pending;
}

public enum EntityStatus { Pending, InProgress, Completed, Failed }

public enum SubQueueStatus { Pending, InProgress, Completed }
