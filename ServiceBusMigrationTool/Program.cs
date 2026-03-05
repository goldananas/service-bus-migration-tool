using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog;
using ServiceBusMigrationTool;
using ServiceBusMigrationTool.Models;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddJsonFile("appsettings.local.json", optional: true)
    .Build();

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(config)
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .WriteTo.File("logs/migration-.log",
        rollingInterval: RollingInterval.Day,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

using var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog());
var logger = loggerFactory.CreateLogger("Program");

var migrationConfig = new NamespaceMigrationConfig();
config.GetSection("NamespaceMigration").Bind(migrationConfig);
bool useNamespaceMigration = IsValidConnectionString(migrationConfig.SourceConnectionString)
    && IsValidConnectionString(migrationConfig.DestinationConnectionString);

var dryRun = config.GetValue<bool>("DryRun");
var stateFilePath = config.GetValue<string>("StateFilePath") ?? "migration-state.json";
var freshRun = config.GetValue<bool>("FreshRun");

var copyConfig = new ServiceBusCopyConfig();
if (!useNamespaceMigration)
{
    config.GetSection("ServiceBusCopy").Bind(copyConfig);

    var errors = ValidateCopyConfig(copyConfig);
    if (errors.Count > 0)
    {
        foreach (var error in errors)
            logger.LogError("Config error: {Error}", error);
        Log.CloseAndFlush();
        return 1;
    }
}

if (freshRun && File.Exists(stateFilePath))
{
    File.Delete(stateFilePath);
    logger.LogInformation("Fresh run requested. Deleted existing state file");
}

var stateManager = new MigrationStateManager(
    stateFilePath, dryRun, loggerFactory.CreateLogger<MigrationStateManager>());

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    logger.LogWarning("Cancellation requested...");
};

try
{
    if (dryRun)
        logger.LogInformation("[dry run] No messages will be sent");

    if (useNamespaceMigration)
    {
        migrationConfig.DryRun = dryRun;
        var migrator = new NamespaceMigrator(
            migrationConfig,
            loggerFactory.CreateLogger<NamespaceMigrator>(),
            loggerFactory.CreateLogger<MessageCopier>(),
            stateManager);
        await migrator.MigrateAsync(cts.Token);
    }
    else
    {
        stateManager.LoadOrCreate("PerOperation");

        for (int i = 0; i < copyConfig.Operations.Count; i++)
        {
            var op = copyConfig.Operations[i];
            op.DryRun = dryRun;

            var destAdmin = new ServiceBusAdministrationClient(op.Destination.ConnectionString);
            bool exists = op.Destination.IsQueue
                ? (await destAdmin.QueueExistsAsync(op.Destination.QueueName!, cts.Token)).Value
                : (await destAdmin.TopicExistsAsync(op.Destination.TopicName!, cts.Token)).Value;

            if (!exists)
            {
                logger.LogWarning("Skipping operation {Index}: destination '{EntityName}' does not exist",
                    i + 1, op.Destination.EntityName);
                continue;
            }

            logger.LogInformation("=== Operation {Index} ===", i + 1);
            await using var copier = new MessageCopier(op, loggerFactory.CreateLogger<MessageCopier>(), stateManager);
            await copier.CopyMessagesAsync(cts.Token);
        }
    }

    return 0;
}
catch (OperationCanceledException)
{
    logger.LogWarning("Operation cancelled");
    return 1;
}
catch (Exception ex)
{
    logger.LogError(ex, "Fatal error");
    return 1;
}
finally
{
    Log.CloseAndFlush();
}

static bool IsValidConnectionString(string connStr) =>
    !string.IsNullOrWhiteSpace(connStr) && !connStr.StartsWith('<');

static List<string> ValidateCopyConfig(ServiceBusCopyConfig config)
{
    var errors = new List<string>();
    if (config.Operations.Count == 0)
    {
        errors.Add("No operations configured. Add at least one entry to ServiceBusCopy:Operations.");
        return errors;
    }

    for (int i = 0; i < config.Operations.Count; i++)
    {
        var op = config.Operations[i];
        var label = $"Operation {i + 1}";

        ValidateConnectionString(errors, op.Source.ConnectionString, label, "Source");
        ValidateConnectionString(errors, op.Destination.ConnectionString, label, "Destination");
        ValidateEndpoint(errors, op.Source, label, "Source", isSource: true);
        ValidateEndpoint(errors, op.Destination, label, "Destination", isSource: false);
    }
    return errors;
}

static void ValidateConnectionString(List<string> errors, string connStr, string opLabel, string role)
{
    if (string.IsNullOrWhiteSpace(connStr) || connStr.StartsWith('<'))
        errors.Add($"[{opLabel}] {role} connection string is not configured.");
}

static void ValidateEndpoint(List<string> errors, ServiceBusEndpoint ep, string opLabel, string role, bool isSource)
{
    if (ep.IsQueue && ep.IsTopic)
        errors.Add($"[{opLabel}] {role} has both QueueName and TopicName set. Use one or the other.");
    else if (!ep.IsQueue && !ep.IsTopic)
        errors.Add($"[{opLabel}] {role} must have either QueueName or TopicName set.");

    if (ep.IsTopic && isSource && string.IsNullOrWhiteSpace(ep.SubscriptionName))
        errors.Add($"[{opLabel}] {role} targets a topic but SubscriptionName is missing.");

    if (ep.IsQueue && !string.IsNullOrWhiteSpace(ep.SubscriptionName))
        errors.Add($"[{opLabel}] {role} targets a queue but has SubscriptionName set (not applicable).");

    if (!isSource && !string.IsNullOrWhiteSpace(ep.SubscriptionName))
        errors.Add($"[{opLabel}] {role} (destination) should not have SubscriptionName set.");
}
