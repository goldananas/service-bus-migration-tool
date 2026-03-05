using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ServiceBusMigrationTool.Models;

namespace ServiceBusMigrationTool;

public class MessageCopier : IAsyncDisposable
{
    private readonly ServiceBusClient _sourceClient;
    private readonly ServiceBusClient _destClient;
    private readonly ServiceBusCopyOperation _settings;
    private readonly ILogger<MessageCopier> _logger;
    private readonly MigrationStateManager? _stateManager;

    public MessageCopier(ServiceBusCopyOperation settings, ILogger<MessageCopier> logger, MigrationStateManager? stateManager = null)
    {
        _settings = settings;
        _logger = logger;
        _stateManager = stateManager;
        _sourceClient = new ServiceBusClient(settings.Source.ConnectionString);
        _destClient = new ServiceBusClient(settings.Destination.ConnectionString);
    }

    public async Task<int> CopyMessagesAsync(CancellationToken cancellationToken = default)
    {
        var entityKey = MigrationStateManager.BuildEntityKey(_settings);

        if (_stateManager != null && _stateManager.IsEntityCompleted(entityKey))
        {
            _logger.LogInformation("Skipping '{EntityKey}': already completed in previous run", entityKey);
            return 0;
        }

        _stateManager?.GetOrAddEntity(entityKey, _settings.Destination.EntityName);

        ServiceBusSender? sender = _settings.DryRun ? null : _destClient.CreateSender(_settings.Destination.EntityName);
        string sourceEntity = _settings.Source.IsQueue
            ? _settings.Source.QueueName!
            : $"{_settings.Source.TopicName}/Subscriptions/{_settings.Source.SubscriptionName}";

        int copied = 0;

        foreach (var subQueue in new[] { SubQueue.None, SubQueue.DeadLetter })
        {
            var receiverOptions = new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                SubQueue = subQueue
            };
            var receiver = _settings.Source.IsQueue
                ? _sourceClient.CreateReceiver(_settings.Source.QueueName!, receiverOptions)
                : _sourceClient.CreateReceiver(_settings.Source.TopicName!, _settings.Source.SubscriptionName!, receiverOptions);

            var sourceLabel = subQueue == SubQueue.DeadLetter
                ? $"{sourceEntity}/$deadletterqueue"
                : sourceEntity;

            long fromSequence = _stateManager?.GetResumeSequenceNumber(entityKey, subQueue) ?? 0;
            if (fromSequence > 0)
                _logger.LogInformation("Resuming '{Source}' from sequence {SequenceNumber}", sourceLabel, fromSequence);
            else
                _logger.LogInformation("Peeking messages from '{Source}'...", sourceLabel);

            int subQueueCopied = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var peeked = await receiver.PeekMessagesAsync(
                    maxMessages: _settings.BatchSize,
                    fromSequenceNumber: fromSequence,
                    cancellationToken: cancellationToken);

                if (peeked.Count == 0)
                    break;

                var batch = new List<ServiceBusMessage>();

                foreach (var msg in peeked)
                {
                    var clone = new ServiceBusMessage(msg);
                    batch.Add(clone);
                    fromSequence = msg.SequenceNumber + 1;
                }

                var count = batch.Count;
                if (_settings.DryRun)
                {
                    subQueueCopied += count;
                    copied += count;
                    _logger.LogInformation("[dry run] Found {Count} messages so far from '{Source}'...", copied, sourceLabel);
                }
                else
                {
                    await sender!.SendMessagesAsync(batch, cancellationToken);
                    subQueueCopied += count;
                    copied += count;
                    _stateManager?.UpdateAfterBatch(entityKey, subQueue, fromSequence, count);
                    _logger.LogInformation("Copied {Count} messages so far from '{Source}'...", copied, sourceLabel);
                }
            }

            _logger.LogDebug("Sub-queue '{Source}': {Count} messages", sourceLabel, subQueueCopied);
            _stateManager?.MarkSubQueueCompleted(entityKey, subQueue);
            await receiver.DisposeAsync();
        }

        if (_settings.DryRun)
            _logger.LogInformation("[dry run] Would copy {Count} message(s) to '{Destination}'", copied, _settings.Destination.EntityName);
        else
            _logger.LogInformation("Done. {Count} message(s) copied to '{Destination}'", copied, _settings.Destination.EntityName);

        _stateManager?.MarkEntityCompleted(entityKey);

        if (sender != null)
            await sender.DisposeAsync();

        return copied;
    }

    public async ValueTask DisposeAsync()
    {
        await _sourceClient.DisposeAsync();
        await _destClient.DisposeAsync();
    }
}
