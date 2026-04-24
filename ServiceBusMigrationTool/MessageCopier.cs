using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
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

        _logger.LogInformation("Starting copy: {SourceHost}/{SourceEntity} -> {DestHost}/{DestEntity}",
            _settings.Source.HostName, sourceEntity,
            _settings.Destination.HostName, _settings.Destination.EntityName);

        long? destCountBefore = _settings.DryRun ? null : await GetDestinationMessageCountAsync(cancellationToken);
        if (destCountBefore.HasValue)
            _logger.LogDebug("Destination '{Destination}' message count before copy: {Count}",
                _settings.Destination.EntityName, destCountBefore.Value);

        int copied = 0;

        var subQueues = _settings.DlqOnly
            ? new[] { SubQueue.DeadLetter }
            : new[] { SubQueue.None, SubQueue.DeadLetter };

        foreach (var subQueue in subQueues)
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
                long firstSeq = peeked[0].SequenceNumber;

                foreach (var msg in peeked)
                {
                    fromSequence = msg.SequenceNumber + 1;

                    var clone = new ServiceBusMessage(msg);

                    // Generate new MessageId to avoid duplicate detection silently dropping messages.
                    // Preserve the original for traceability.
                    if (!string.IsNullOrEmpty(msg.MessageId))
                        clone.ApplicationProperties["OriginalMessageId"] = msg.MessageId;
                    clone.MessageId = Guid.NewGuid().ToString();

                    batch.Add(clone);

                    _logger.LogDebug("  Peek: seq={SequenceNumber}, msgId={MessageId}, contentType={ContentType}, bodySize={BodySize}",
                        msg.SequenceNumber, msg.MessageId, msg.ContentType ?? "(none)", msg.Body?.ToMemory().Length ?? 0);
                }

                long lastSeq = peeked[^1].SequenceNumber;
                var count = batch.Count;

                if (_settings.DryRun)
                {
                    subQueueCopied += count;
                    copied += count;
                    _logger.LogInformation("[dry run] Found {Count} messages so far from '{Source}'...", copied, sourceLabel);
                }
                else
                {
                    int skipped = await SendBatchWithFallback(sender!, batch, sourceLabel, cancellationToken);
                    count -= skipped;

                    subQueueCopied += count;
                    copied += count;
                    _stateManager?.UpdateAfterBatch(entityKey, subQueue, fromSequence, count);

                    _logger.LogInformation("Sent {BatchCount} messages (seq {FirstSeq}..{LastSeq}), {TotalCount} total so far from '{Source}'",
                        count, firstSeq, lastSeq, copied, sourceLabel);
                }
            }

            _logger.LogInformation("Sub-queue '{Source}' finished: {Count} message(s)", sourceLabel, subQueueCopied);
            _stateManager?.MarkSubQueueCompleted(entityKey, subQueue);
            await receiver.DisposeAsync();
        }

        if (_settings.DryRun)
        {
            _logger.LogInformation("[dry run] Would copy {Count} message(s) to '{Destination}'", copied, _settings.Destination.EntityName);
        }
        else
        {
            _logger.LogInformation("Done. {Count} message(s) sent to '{Destination}'", copied, _settings.Destination.EntityName);
            await VerifyDestinationCountAsync(copied, destCountBefore, cancellationToken);
        }

        _stateManager?.MarkEntityCompleted(entityKey);

        if (sender != null)
            await sender.DisposeAsync();

        return copied;
    }

    private async Task<int> SendBatchWithFallback(
        ServiceBusSender sender, List<ServiceBusMessage> batch, string sourceLabel, CancellationToken cancellationToken)
    {
        // Group by PartitionKey/SessionId to avoid InvalidOperationException on partitioned entities
        var groups = batch.GroupBy(m => m.PartitionKey ?? m.SessionId ?? string.Empty);
        int skipped = 0;

        foreach (var group in groups)
        {
            var messages = group.ToList();
            try
            {
                await sender.SendMessagesAsync(messages, cancellationToken);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("no data in it") && messages.Count > 1)
            {
                _logger.LogWarning("Batch send failed for group from '{Source}', falling back to individual sends", sourceLabel);
                foreach (var msg in messages)
                {
                    try
                    {
                        await sender.SendMessagesAsync([msg], cancellationToken);
                    }
                    catch (InvalidOperationException ex2) when (ex2.Message.Contains("no data in it"))
                    {
                        _logger.LogWarning("Skipping unsendable message (msgId={MessageId}) from '{Source}': {Error}",
                            msg.MessageId, sourceLabel, ex2.Message);
                        skipped++;
                    }
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("no data in it") && messages.Count == 1)
            {
                _logger.LogWarning("Skipping unsendable message (msgId={MessageId}) from '{Source}': {Error}",
                    messages[0].MessageId, sourceLabel, ex.Message);
                skipped++;
            }
        }

        return skipped;
    }

    private async Task<long?> GetDestinationMessageCountAsync(CancellationToken cancellationToken)
    {
        try
        {
            var admin = new ServiceBusAdministrationClient(_settings.Destination.ConnectionString);
            if (_settings.Destination.IsQueue)
            {
                var props = await admin.GetQueueRuntimePropertiesAsync(
                    _settings.Destination.QueueName!, cancellationToken);
                return props.Value.ActiveMessageCount;
            }
            else
            {
                return await GetTopicTotalMessageCountAsync(admin, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not query destination message count (Manage permission may be missing)");
            return null;
        }
    }

    private async Task<long> GetTopicTotalMessageCountAsync(ServiceBusAdministrationClient admin, CancellationToken cancellationToken)
    {
        long total = 0;
        await foreach (var sub in admin.GetSubscriptionsRuntimePropertiesAsync(
            _settings.Destination.TopicName!, cancellationToken))
        {
            total += sub.ActiveMessageCount + sub.DeadLetterMessageCount;
        }
        return total;
    }

    private async Task VerifyDestinationCountAsync(int sentCount, long? countBefore, CancellationToken cancellationToken)
    {
        if (sentCount == 0)
            return;

        try
        {
            var admin = new ServiceBusAdministrationClient(_settings.Destination.ConnectionString);
            long countAfter;

            if (_settings.Destination.IsQueue)
            {
                var props = await admin.GetQueueRuntimePropertiesAsync(
                    _settings.Destination.QueueName!, cancellationToken);
                countAfter = props.Value.ActiveMessageCount;
            }
            else
            {
                countAfter = await GetTopicTotalMessageCountAsync(admin, cancellationToken);
            }

            var entityType = _settings.Destination.IsQueue ? "queue" : "topic (all subscriptions)";
            _logger.LogInformation("Destination {EntityType} '{Destination}' message count: {CountAfter}",
                entityType, _settings.Destination.EntityName, countAfter);

            if (countBefore.HasValue)
            {
                var increase = countAfter - countBefore.Value;
                if (increase < sentCount)
                {
                    _logger.LogWarning(
                        "Destination '{Destination}' message count increased by {Increase}, but {Sent} messages were sent. " +
                        "Possible causes: duplicate detection dropping messages, TTL expiry, or messages consumed by another reader",
                        _settings.Destination.EntityName, increase, sentCount);
                }
                else
                {
                    _logger.LogInformation("Verified: destination count increased by {Increase} (sent {Sent})",
                        increase, sentCount);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not verify destination message count (Manage permission may be missing)");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _sourceClient.DisposeAsync();
        await _destClient.DisposeAsync();
    }
}
