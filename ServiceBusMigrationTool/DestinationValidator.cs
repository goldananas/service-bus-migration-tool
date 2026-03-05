using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;

namespace ServiceBusMigrationTool;

public class DestinationValidator
{
    private readonly ServiceBusAdministrationClient _adminClient;
    private readonly ILogger _logger;

    public DestinationValidator(string connectionString, ILogger logger)
    {
        _adminClient = new ServiceBusAdministrationClient(connectionString);
        _logger = logger;
    }

    public async Task<bool> QueueExistsAsync(string queueName, CancellationToken ct)
    {
        var exists = (await _adminClient.QueueExistsAsync(queueName, ct)).Value;
        if (!exists)
            _logger.LogWarning("Destination queue '{QueueName}' does not exist, skipping", queueName);
        return exists;
    }

    public async Task<bool> TopicExistsAsync(string topicName, CancellationToken ct)
    {
        var exists = (await _adminClient.TopicExistsAsync(topicName, ct)).Value;
        if (!exists)
            _logger.LogWarning("Destination topic '{TopicName}' does not exist, skipping", topicName);
        return exists;
    }

    public async Task<bool> SubscriptionExistsAsync(string topicName, string subscriptionName, CancellationToken ct)
    {
        var exists = (await _adminClient.SubscriptionExistsAsync(topicName, subscriptionName, ct)).Value;
        if (!exists)
            _logger.LogWarning("Destination subscription '{TopicName}/Subscriptions/{SubscriptionName}' does not exist, skipping",
                topicName, subscriptionName);
        return exists;
    }
}
