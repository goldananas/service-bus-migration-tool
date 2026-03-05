# Service Bus CLI

A .NET 8 CLI tool for managing Azure Service Bus messages. Currently supports copying messages from one queue to another (across namespaces).

## Features

- **Copy messages** between queues across different Service Bus namespaces
- **Copy from Dead Letter Queue (DLQ)** -- recover or migrate dead-lettered messages
- Non-destructive read (peek) from the source queue -- messages are not consumed
- Supports cancellation via Ctrl+C

## Configuration

The tool uses `appsettings.json` as a template with placeholder values. To configure your connection strings, create an `appsettings.local.json` file (git-ignored) alongside it:

```bash
cp ServiceBusCli/appsettings.json ServiceBusCli/appsettings.local.json
```

Then edit `appsettings.local.json` with your actual connection strings:

```json
{
  "ServiceBusCopy": {
    "Source": {
      "ConnectionString": "Endpoint=sb://source-ns.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...",
      "QueueName": "my-source-queue"
    },
    "Destination": {
      "ConnectionString": "Endpoint=sb://dest-ns.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...",
      "QueueName": "my-dest-queue"
    }
  }
}
```

> **Note:** `appsettings.local.json` is listed in `.gitignore` — your connection strings will never be committed.

### Getting connection strings

1. Go to your Service Bus namespace in the Azure Portal
2. Navigate to **Shared access policies**
3. Select the policy (e.g. `RootManageSharedAccessKey`) or create one with the required claims
4. Copy the **Primary Connection String**

## Usage

```bash
# From the repo root
dotnet run --project ServiceBusCli
```

## How it works

By default, the tool reads from the **Dead Letter Queue (DLQ)** of the source queue. To read from the main queue instead, set `"UseDlq": false` on the source endpoint in the config.

The tool **peeks** messages (non-destructive) and sends copies to the destination queue. The original messages remain untouched.

Messages are processed in batches of 100. All message properties (body, content type, correlation ID, custom properties, etc.) are preserved during the copy.
