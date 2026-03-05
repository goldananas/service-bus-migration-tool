# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build
dotnet build ServiceBusCli

# Run
dotnet run --project ServiceBusCli
```

No test framework is configured. No linter or formatter is configured beyond the defaults in the .csproj (nullable enabled, implicit usings).

## Architecture

Single-project .NET 8 console app (`ServiceBusCli/`) that copies Azure Service Bus messages between queues.

**Entry point:** `Program.cs` — loads configuration, validates settings, wires up cancellation (Ctrl+C), and runs `MessageCopier`.

**Core flow:**
1. `Program.cs` builds config from `appsettings.json` + optional `appsettings.local.json`, binds to `ServiceBusCopySettings`, validates connection strings aren't placeholders, then runs `MessageCopier.CopyMessagesAsync()`.
2. `MessageCopier` peeks messages in batches of 100 from the source queue (non-destructive) and sends copies to the destination. Implements `IAsyncDisposable` to clean up `ServiceBusClient` instances.

**Models:**
- `ServiceBusCopySettings` — holds `Source` and `Destination` endpoints
- `ServiceBusEndpoint` — `ConnectionString`, `QueueName`, `UseDlq` (default: `true`)

No dependency injection container — settings are instantiated directly and passed to `MessageCopier`.

## Configuration

- `appsettings.json` — committed template with placeholder values
- `appsettings.local.json` — git-ignored, contains real connection strings (overrides appsettings.json)
- Config files are copied to output directory via .csproj `<Content>` items with `PreserveNewest`

## Dependencies

- `Azure.Messaging.ServiceBus` — Azure SDK for peek/send operations
- `Microsoft.Extensions.Configuration.*` — JSON config loading and binding
