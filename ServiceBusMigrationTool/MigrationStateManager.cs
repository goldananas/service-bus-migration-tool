using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ServiceBusMigrationTool.Models;

namespace ServiceBusMigrationTool;

public class MigrationStateManager
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly string _stateFilePath;
    private readonly bool _enabled;
    private readonly ILogger<MigrationStateManager> _logger;
    private MigrationState _state = new();
    private readonly object _writeLock = new();

    public MigrationStateManager(string stateFilePath, bool dryRun, ILogger<MigrationStateManager> logger)
    {
        _stateFilePath = stateFilePath;
        _enabled = !dryRun;
        _logger = logger;
    }

    public MigrationState LoadOrCreate(string mode)
    {
        if (!_enabled)
            return _state;

        if (File.Exists(_stateFilePath))
        {
            var json = File.ReadAllText(_stateFilePath);
            _state = JsonSerializer.Deserialize<MigrationState>(json, JsonOptions) ?? new MigrationState();
            _logger.LogInformation("Resuming from state file '{Path}' with {Count} tracked entities",
                _stateFilePath, _state.Entities.Count);
        }
        else
        {
            _state = new MigrationState { StartedAt = DateTime.UtcNow, Mode = mode };
        }

        return _state;
    }

    public EntityMigrationState GetOrAddEntity(string entityKey, string destinationEntity)
    {
        lock (_writeLock)
        {
            if (_state.Entities.TryGetValue(entityKey, out var existing))
                return existing;

            var entity = new EntityMigrationState
            {
                EntityKey = entityKey,
                DestinationEntity = destinationEntity
            };
            _state.Entities[entityKey] = entity;
            return entity;
        }
    }

    public bool IsEntityCompleted(string entityKey)
    {
        lock (_writeLock)
        {
            return _state.Entities.TryGetValue(entityKey, out var entity)
                   && entity.Status == EntityStatus.Completed;
        }
    }

    public long GetResumeSequenceNumber(string entityKey, SubQueue subQueue)
    {
        lock (_writeLock)
        {
            if (!_state.Entities.TryGetValue(entityKey, out var entity))
                return 0;

            var sqState = subQueue == SubQueue.DeadLetter ? entity.DeadLetterQueue : entity.MainQueue;
            return sqState.LastSuccessfulSequenceNumber;
        }
    }

    public void UpdateAfterBatch(string entityKey, SubQueue subQueue, long lastSequenceNumber, int batchCount)
    {
        if (!_enabled)
            return;

        lock (_writeLock)
        {
            var entity = _state.Entities[entityKey];
            var sqState = subQueue == SubQueue.DeadLetter ? entity.DeadLetterQueue : entity.MainQueue;
            sqState.LastSuccessfulSequenceNumber = lastSequenceNumber;
            sqState.MessagesCopied += batchCount;
            sqState.Status = SubQueueStatus.InProgress;
            entity.TotalMessagesCopied += batchCount;
            entity.Status = EntityStatus.InProgress;
            _state.LastUpdatedAt = DateTime.UtcNow;
            PersistToDisk();
        }
    }

    public void MarkSubQueueCompleted(string entityKey, SubQueue subQueue)
    {
        if (!_enabled)
            return;

        lock (_writeLock)
        {
            var entity = _state.Entities[entityKey];
            var sqState = subQueue == SubQueue.DeadLetter ? entity.DeadLetterQueue : entity.MainQueue;
            sqState.Status = SubQueueStatus.Completed;
            _state.LastUpdatedAt = DateTime.UtcNow;
            PersistToDisk();
        }
    }

    public void MarkEntityCompleted(string entityKey)
    {
        if (!_enabled)
            return;

        lock (_writeLock)
        {
            var entity = _state.Entities[entityKey];
            entity.Status = EntityStatus.Completed;
            _state.LastUpdatedAt = DateTime.UtcNow;
            PersistToDisk();
        }
    }

    public void MarkEntityFailed(string entityKey)
    {
        if (!_enabled)
            return;

        lock (_writeLock)
        {
            if (_state.Entities.TryGetValue(entityKey, out var entity))
            {
                entity.Status = EntityStatus.Failed;
                _state.LastUpdatedAt = DateTime.UtcNow;
                PersistToDisk();
            }
        }
    }

    public static string BuildEntityKey(ServiceBusCopyOperation op)
    {
        var src = op.Source;
        return src.IsQueue
            ? $"queue:{src.QueueName}"
            : $"topic:{src.TopicName}/sub:{src.SubscriptionName}";
    }

    private void PersistToDisk()
    {
        var json = JsonSerializer.Serialize(_state, JsonOptions);
        var tempPath = _stateFilePath + ".tmp";
        File.WriteAllText(tempPath, json);
        File.Move(tempPath, _stateFilePath, overwrite: true);
    }
}
