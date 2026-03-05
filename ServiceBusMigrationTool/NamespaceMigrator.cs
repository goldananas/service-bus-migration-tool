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
    }

    public async Task<int> MigrateAsync(CancellationToken cancellationToken)
    {
        _stateManager.LoadOrCreate("NamespaceMigration");

        var sourceAdmin = new ServiceBusAdministrationClient(_config.SourceConnectionString);
        var destValidator = new DestinationValidator(_config.DestinationConnectionString, _logger);
        var operations = new List<ServiceBusCopyOperation>();

        _logger.LogInformation("Discovering queues...");
        await foreach (var queue in sourceAdmin.GetQueuesRuntimePropertiesAsync(cancellationToken))
        {
            var total = queue.ActiveMessageCount + queue.DeadLetterMessageCount;
            if (total > 0)
            {
                if (!await destValidator.QueueExistsAsync(queue.Name, cancellationToken))
                    continue;

                operations.Add(BuildQueueOperation(queue.Name));
                _logger.LogInformation("  Queue '{QueueName}': {ActiveCount} active, {DlqCount} DLQ",
                    queue.Name, queue.ActiveMessageCount, queue.DeadLetterMessageCount);
            }
        }

        _logger.LogInformation("Discovering topics and subscriptions...");
        await foreach (var topic in sourceAdmin.GetTopicsAsync(cancellationToken))
        {
            if (!await destValidator.TopicExistsAsync(topic.Name, cancellationToken))
                continue;

            await foreach (var sub in sourceAdmin.GetSubscriptionsRuntimePropertiesAsync(topic.Name, cancellationToken))
            {
                var total = sub.ActiveMessageCount + sub.DeadLetterMessageCount;
                if (total > 0)
                {
                    operations.Add(BuildTopicOperation(topic.Name, sub.SubscriptionName));
                    _logger.LogInformation("  Topic '{TopicName}/Subscriptions/{SubscriptionName}': {ActiveCount} active, {DlqCount} DLQ",
                        topic.Name, sub.SubscriptionName, sub.ActiveMessageCount, sub.DeadLetterMessageCount);
                }
            }
        }

        if (operations.Count == 0)
        {
            _logger.LogInformation("No messages found in any queue or subscription");
            return 0;
        }

        if (_config.DryRun)
        {
            _logger.LogInformation("Dry run: {Count} operation(s) would be executed. No messages copied", operations.Count);
            return 0;
        }

        _logger.LogInformation("Starting {Count} copy operation(s) with max concurrency {MaxConcurrency}...",
            operations.Count, _config.MaxConcurrency);

        int totalCopied = 0;
        int failed = 0;
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
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _stateManager.MarkEntityFailed(entityKey);
                _logger.LogError(ex, "Failed to copy '{EntityKey}'", entityKey);
                Interlocked.Increment(ref failed);
            }
        });

        _logger.LogInformation("Migration complete. {TotalCopied} total message(s) copied. {Failed} operation(s) failed",
            totalCopied, failed);
        return totalCopied;
    }

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
