using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using ServiceBusMigrationTool.Models;

namespace ServiceBusMigrationTool;

public class NamespaceMigrator
{
    private readonly NamespaceMigrationConfig _config;
    private readonly ILogger<NamespaceMigrator> _logger;
    private readonly ILogger<MessageCopier> _copierLogger;
    private readonly MigrationStateManager _stateManager;
    private readonly HashSet<string> _excludedEntities;

    public NamespaceMigrator(
        NamespaceMigrationConfig config,
        ILogger<NamespaceMigrator> logger,
        ILogger<MessageCopier> copierLogger,
        MigrationStateManager stateManager)
    {
        _config = config;
        _logger = logger;
        _copierLogger = copierLogger;
        _stateManager = stateManager;
        _excludedEntities = new HashSet<string>(config.ExcludeEntities, StringComparer.OrdinalIgnoreCase);
    }

    private record DiscoveredEntity(string Name, long Active, long Dlq, string? SkipReason);

    public async Task<int> MigrateAsync(CancellationToken cancellationToken)
    {
        _stateManager.LoadOrCreate("NamespaceMigration");

        var sourceAdmin = new ServiceBusAdministrationClient(_config.SourceConnectionString);
        var destValidator = new DestinationValidator(_config.DestinationConnectionString, _logger);
        var operations = new List<ServiceBusCopyOperation>();
        var allEntities = new List<DiscoveredEntity>();

        _logger.LogInformation("Discovering queues...");
        await foreach (var queue in sourceAdmin.GetQueuesRuntimePropertiesAsync(cancellationToken))
        {
            var total = queue.ActiveMessageCount + queue.DeadLetterMessageCount;

            if (_excludedEntities.Contains(queue.Name))
            {
                _logger.LogInformation("  Queue '{QueueName}': {ActiveCount} active, {DlqCount} DLQ — excluded",
                    queue.Name, queue.ActiveMessageCount, queue.DeadLetterMessageCount);
                allEntities.Add(new(queue.Name, queue.ActiveMessageCount, queue.DeadLetterMessageCount, "EXCLUDED"));
                continue;
            }

            if (total == 0)
            {
                _logger.LogInformation("  Queue '{QueueName}': empty", queue.Name);
                allEntities.Add(new(queue.Name, 0, 0, "EMPTY"));
                continue;
            }

            if (!await destValidator.QueueExistsAsync(queue.Name, cancellationToken))
            {
                allEntities.Add(new(queue.Name, queue.ActiveMessageCount, queue.DeadLetterMessageCount, "NO DEST"));
                continue;
            }

            operations.Add(BuildQueueOperation(queue.Name));
            allEntities.Add(new(queue.Name, queue.ActiveMessageCount, queue.DeadLetterMessageCount, null));
            _logger.LogInformation("  Queue '{QueueName}': {ActiveCount} active, {DlqCount} DLQ",
                queue.Name, queue.ActiveMessageCount, queue.DeadLetterMessageCount);
        }

        _logger.LogInformation("Discovering topics and subscriptions...");
        await foreach (var topic in sourceAdmin.GetTopicsAsync(cancellationToken))
        {
            if (!await destValidator.TopicExistsAsync(topic.Name, cancellationToken))
            {
                // Topic doesn't exist at destination — still enumerate subscriptions to report them
                await foreach (var sub in sourceAdmin.GetSubscriptionsRuntimePropertiesAsync(topic.Name, cancellationToken))
                {
                    var entityPath = $"{topic.Name}/Subscriptions/{sub.SubscriptionName}";
                    allEntities.Add(new(entityPath, sub.ActiveMessageCount, sub.DeadLetterMessageCount, "NO DEST (topic)"));
                    _logger.LogInformation("  Subscription '{EntityPath}': {ActiveCount} active, {DlqCount} DLQ — destination topic missing",
                        entityPath, sub.ActiveMessageCount, sub.DeadLetterMessageCount);
                }
                continue;
            }

            await foreach (var sub in sourceAdmin.GetSubscriptionsRuntimePropertiesAsync(topic.Name, cancellationToken))
            {
                var entityPath = $"{topic.Name}/Subscriptions/{sub.SubscriptionName}";
                var total = sub.ActiveMessageCount + sub.DeadLetterMessageCount;

                if (_excludedEntities.Contains(entityPath))
                {
                    _logger.LogInformation("  Subscription '{EntityPath}': {ActiveCount} active, {DlqCount} DLQ — excluded",
                        entityPath, sub.ActiveMessageCount, sub.DeadLetterMessageCount);
                    allEntities.Add(new(entityPath, sub.ActiveMessageCount, sub.DeadLetterMessageCount, "EXCLUDED"));
                    continue;
                }

                if (total == 0)
                {
                    _logger.LogInformation("  Subscription '{EntityPath}': empty", entityPath);
                    allEntities.Add(new(entityPath, 0, 0, "EMPTY"));
                    continue;
                }

                if (!await destValidator.SubscriptionExistsAsync(topic.Name, sub.SubscriptionName, cancellationToken))
                {
                    allEntities.Add(new(entityPath, sub.ActiveMessageCount, sub.DeadLetterMessageCount, "NO DEST"));
                    continue;
                }

                operations.Add(BuildTopicOperation(topic.Name, sub.SubscriptionName));
                allEntities.Add(new(entityPath, sub.ActiveMessageCount, sub.DeadLetterMessageCount, null));
                _logger.LogInformation("  Subscription '{EntityPath}': {ActiveCount} active, {DlqCount} DLQ",
                    entityPath, sub.ActiveMessageCount, sub.DeadLetterMessageCount);
            }
        }

        if (_config.DryRun)
        {
            PrintDryRunReport(allEntities);
            return 0;
        }

        if (operations.Count == 0)
        {
            _logger.LogInformation("No copyable entities found (all empty, excluded, or missing destination)");
            return 0;
        }

        _logger.LogInformation("Starting {Count} copy operation(s) with max concurrency {MaxConcurrency}...",
            operations.Count, _config.MaxConcurrency);

        int totalCopied = 0;
        int failed = 0;
        var results = new System.Collections.Concurrent.ConcurrentBag<(string EntityKey, int Count, bool Success)>();

        await Parallel.ForEachAsync(operations, new ParallelOptions
        {
            MaxDegreeOfParallelism = _config.MaxConcurrency,
            CancellationToken = cancellationToken
        }, async (operation, ct) =>
        {
            var entityKey = MigrationStateManager.BuildEntityKey(operation);
            try
            {
                await using var copier = new MessageCopier(operation, _copierLogger, _stateManager);
                var count = await copier.CopyMessagesAsync(ct);
                Interlocked.Add(ref totalCopied, count);
                results.Add((entityKey, count, true));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _stateManager.MarkEntityFailed(entityKey);
                _logger.LogError(ex, "Failed to copy '{EntityKey}'", entityKey);
                Interlocked.Increment(ref failed);
                results.Add((entityKey, 0, false));
            }
        });

        _logger.LogInformation("=== Migration Summary ===");
        foreach (var r in results.OrderBy(r => r.EntityKey))
        {
            if (r.Success)
                _logger.LogInformation("  {EntityKey}: {Count} message(s) sent", r.EntityKey, r.Count);
            else
                _logger.LogError("  {EntityKey}: FAILED", r.EntityKey);
        }
        _logger.LogInformation("Total: {TotalCopied} message(s) sent, {Failed} operation(s) failed",
            totalCopied, failed);

        return totalCopied;
    }

    private void PrintDryRunReport(List<DiscoveredEntity> entities)
    {
        var sorted = entities.OrderByDescending(e => e.Active + e.Dlq).ToList();

        var copyable = sorted.Where(e => e.SkipReason == null).ToList();
        var skipped = sorted.Where(e => e.SkipReason != null).ToList();

        _logger.LogInformation("=== Dry Run Report — Will Copy (sorted by total messages) ===");
        _logger.LogInformation("");

        if (copyable.Count == 0)
        {
            _logger.LogInformation("  (none)");
        }
        else
        {
            foreach (var e in copyable)
            {
                var total = e.Active + e.Dlq;
                var risk = GetRiskLevel(total);
                _logger.LogInformation("  [{Risk}]  {Name}: {Active} active, {Dlq} DLQ ({Total} total)",
                    risk, e.Name, e.Active, e.Dlq, total);
            }
        }

        _logger.LogInformation("");
        _logger.LogInformation("=== Skipped Entities ===");
        _logger.LogInformation("");

        if (skipped.Count == 0)
        {
            _logger.LogInformation("  (none)");
        }
        else
        {
            foreach (var e in skipped)
            {
                var total = e.Active + e.Dlq;
                _logger.LogInformation("  [{Reason}]  {Name}: {Active} active, {Dlq} DLQ ({Total} total)",
                    e.SkipReason, e.Name, e.Active, e.Dlq, total);
            }
        }

        var grandTotal = sorted.Sum(e => e.Active + e.Dlq);
        var copyableTotal = copyable.Sum(e => e.Active + e.Dlq);

        _logger.LogInformation("");
        _logger.LogInformation("Summary: {AllCount} entities discovered, {CopyableCount} will be copied ({CopyableMessages} messages), {SkippedCount} skipped",
            sorted.Count, copyable.Count, copyableTotal, skipped.Count);
        _logger.LogInformation("Grand total across all entities: {GrandTotal} messages", grandTotal);

        var blockers = copyable.Count(e => GetRiskLevel(e.Active + e.Dlq) is "BLOCKER" or "HIGH");
        if (blockers > 0)
            _logger.LogWarning("{Count} entity(ies) flagged as HIGH or BLOCKER — consider adding them to ExcludeEntities", blockers);
    }

    private static string GetRiskLevel(long totalMessages) => totalMessages switch
    {
        > 500_000 => "BLOCKER",
        > 100_000 => "HIGH",
        > 10_000 => "MODERATE",
        _ => "OK"
    };

    private ServiceBusCopyOperation BuildQueueOperation(string queueName) => new()
    {
        BatchSize = _config.BatchSize,
        Source = new ServiceBusEndpoint
        {
            ConnectionString = _config.SourceConnectionString,
            QueueName = queueName
        },
        Destination = new ServiceBusEndpoint
        {
            ConnectionString = _config.DestinationConnectionString,
            QueueName = queueName
        }
    };

    private ServiceBusCopyOperation BuildTopicOperation(string topicName, string subscriptionName) => new()
    {
        BatchSize = _config.BatchSize,
        Source = new ServiceBusEndpoint
        {
            ConnectionString = _config.SourceConnectionString,
            TopicName = topicName,
            SubscriptionName = subscriptionName
        },
        Destination = new ServiceBusEndpoint
        {
            ConnectionString = _config.DestinationConnectionString,
            TopicName = topicName
        }
    };
}
