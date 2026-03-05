using Microsoft.Extensions.Configuration;
using ServiceBusCli;
using ServiceBusCli.Models;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddJsonFile("appsettings.local.json", optional: true)
    .Build();

var settings = new ServiceBusCopySettings();
config.GetSection("ServiceBusCopy").Bind(settings);

if (string.IsNullOrWhiteSpace(settings.Source.ConnectionString) ||
    settings.Source.ConnectionString.StartsWith('<'))
{
    Console.Error.WriteLine("Error: Source connection string is not configured.");
    Console.Error.WriteLine("Update appsettings.json or create appsettings.local.json with your connection strings.");
    return 1;
}

if (string.IsNullOrWhiteSpace(settings.Destination.ConnectionString) ||
    settings.Destination.ConnectionString.StartsWith('<'))
{
    Console.Error.WriteLine("Error: Destination connection string is not configured.");
    return 1;
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nCancellation requested...");
};

try
{
    await using var copier = new MessageCopier(settings);
    await copier.CopyMessagesAsync(cts.Token);
    return 0;
}
catch (OperationCanceledException)
{
    Console.WriteLine("Operation cancelled.");
    return 1;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Error: {ex.Message}");
    return 1;
}
