# Service Bus Migration Tool

A .NET 8 CLI tool for migrating Azure Service Bus messages. Supports full namespace migration with parallel execution, or targeted per-operation copy between queues and topics. Copies both active and dead-letter messages, resending them as normal messages to the destination.

## Features

- **Namespace migration** -- auto-discover all queues and topic subscriptions, copy messages in parallel
- **Per-operation copy** -- manually define individual copy operations for targeted migrations
- **Topic/subscription support** -- read from subscriptions, send to topics
- Copies both active messages and DLQ messages, resent as normal messages
- Non-destructive read (peek) from the source -- messages are not consumed
- Configurable batch size and concurrency
- Supports cancellation via Ctrl+C

### Reliability

- **Safe by default** -- if a source entity (queue or topic) doesn't exist in the destination, the tool logs a warning and skips it. It will never crash because of a missing destination.
- **Resume after failure** -- progress is saved to a state file (`migration-state.json`) after each batch. If the tool is interrupted or crashes, re-running it picks up where it left off: completed entities are skipped, in-progress ones resume from the last successful sequence number.
- **Error isolation** -- in namespace migration mode, a failure on one entity doesn't stop the others. The failed entity is marked in the state file and all remaining operations continue.
- **Structured logging** -- all output includes timestamps and log levels, written to both the console and daily rolling log files (`logs/migration-YYYYMMDD.log`) for post-migration review.
- **Atomic state writes** -- the state file is written atomically (temp file + rename) to prevent corruption if the process is killed mid-write.

## Configuration

The tool uses `appsettings.json` as a template with placeholder values. To configure your connection strings, create an `appsettings.local.json` file (git-ignored) alongside it:

```bash
cp ServiceBusMigrationTool/appsettings.json ServiceBusMigrationTool/appsettings.local.json
```

Then edit `appsettings.local.json` with your actual connection strings.

> **Note:** `appsettings.local.json` is listed in `.gitignore` — your connection strings will never be committed.

### Namespace migration (recommended)

Provide source and destination namespace connection strings. The tool will auto-discover all queues and topic subscriptions with messages and copy them in parallel:

```json
{
  "NamespaceMigration": {
    "SourceConnectionString": "Endpoint=sb://source-ns.servicebus.windows.net/;...",
    "DestinationConnectionString": "Endpoint=sb://dest-ns.servicebus.windows.net/;...",
    "BatchSize": 10000,
    "MaxConcurrency": 5
  }
}
```

If a destination queue or topic doesn't exist, the tool will log a warning and skip it -- it won't fail.

### Per-operation copy (fallback)

If no `NamespaceMigration` config is provided, the tool falls back to manually-defined operations:

```json
{
  "ServiceBusCopy": {
    "Operations": [
      {
        "Source": {
          "ConnectionString": "Endpoint=sb://source-ns.servicebus.windows.net/;...",
          "QueueName": "my-source-queue"
        },
        "Destination": {
          "ConnectionString": "Endpoint=sb://dest-ns.servicebus.windows.net/;...",
          "QueueName": "my-dest-queue"
        }
      },
      {
        "Source": {
          "ConnectionString": "Endpoint=sb://source-ns.servicebus.windows.net/;...",
          "TopicName": "my-topic",
          "SubscriptionName": "my-subscription"
        },
        "Destination": {
          "ConnectionString": "Endpoint=sb://dest-ns.servicebus.windows.net/;...",
          "TopicName": "my-dest-topic"
        }
      }
    ]
  }
}
```

### Resume and state

The tool automatically saves progress to a state file after each batch send. By default this file is `migration-state.json` in the working directory.

```json
{
  "StateFilePath": "migration-state.json",
  "FreshRun": false
}
```

- If the tool is interrupted (Ctrl+C, crash, network error), just run it again -- it will skip completed entities and resume in-progress ones.
- Set `"FreshRun": true` to delete the state file and start a completely fresh migration.
- The state file is not created during dry runs.

### Logging

Logs are written to the console and to daily rolling files in the `logs/` directory.

Console output:
```
[14:30:15 INF] Discovering queues...
[14:30:15 INF]   Queue 'orders': 1500 active, 12 DLQ
[14:30:16 WRN] Destination queue 'legacy-queue' does not exist, skipping
[14:30:16 INF] Copied 100 messages so far from 'orders'...
```

Log level defaults can be overridden in `appsettings.json` or `appsettings.local.json` via the `Serilog` section:

```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Azure": "Warning"
      }
    }
  }
}
```

### Getting connection strings

1. Go to your Service Bus namespace in the Azure Portal
2. Navigate to **Shared access policies**
3. Select the policy (e.g. `RootManageSharedAccessKey`) or create one with the required claims
4. Copy the **Primary Connection String**

## Usage

```bash
# From the repo root
dotnet run --project ServiceBusMigrationTool
```

## How it works

For each source entity, the tool peeks messages from both the main queue and the dead-letter queue, cloning and sending them all to the destination as normal messages. Original messages remain untouched in the source.

In namespace migration mode, the tool uses the Service Bus Administration API to discover all queues and topic subscriptions, then copies messages in parallel (default: 5 concurrent operations).

All message properties (body, content type, correlation ID, custom properties, etc.) are preserved during the copy.
