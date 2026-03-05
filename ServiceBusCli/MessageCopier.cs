using Azure.Messaging.ServiceBus;
using ServiceBusCli.Models;

namespace ServiceBusCli;

public class MessageCopier : IAsyncDisposable
{
    private readonly ServiceBusClient _sourceClient;
    private readonly ServiceBusClient _destClient;
    private readonly ServiceBusCopySettings _settings;

    public MessageCopier(ServiceBusCopySettings settings)
    {
        _settings = settings;
        _sourceClient = new ServiceBusClient(settings.Source.ConnectionString);
        _destClient = new ServiceBusClient(settings.Destination.ConnectionString);
    }

    public async Task<int> CopyMessagesAsync(CancellationToken cancellationToken = default)
    {
        var receiverOptions = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            SubQueue = _settings.Source.UseDlq ? SubQueue.DeadLetter : SubQueue.None
        };
        var receiver = _sourceClient.CreateReceiver(_settings.Source.QueueName, receiverOptions);

        var sender = _destClient.CreateSender(_settings.Destination.QueueName);

        int copied = 0;
        var sourceLabel = _settings.Source.UseDlq
            ? $"{_settings.Source.QueueName}/$deadletterqueue"
            : _settings.Source.QueueName;

        Console.WriteLine($"Peeking messages from '{sourceLabel}'...");

        // Peek messages in batches (non-destructive read)
        long fromSequence = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var peeked = await receiver.PeekMessagesAsync(
                maxMessages: 100,
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

            await sender.SendMessagesAsync(batch, cancellationToken);
            copied += batch.Count;

            Console.WriteLine($"  Copied {copied} messages so far...");
        }

        Console.WriteLine($"Done. {copied} message(s) copied to '{_settings.Destination.QueueName}'.");

        await sender.DisposeAsync();
        await receiver.DisposeAsync();

        return copied;
    }

    public async ValueTask DisposeAsync()
    {
        await _sourceClient.DisposeAsync();
        await _destClient.DisposeAsync();
    }
}
