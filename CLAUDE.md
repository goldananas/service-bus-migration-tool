# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build
dotnet build ServiceBusMigrationTool

# Run
dotnet run --project ServiceBusMigrationTool
```

No test framework is configured. No linter or formatter is configured beyond the defaults in the .csproj (nullable enabled, implicit usings).

## Architecture

Single-project .NET 8 console app (`ServiceBusMigrationTool/`) that copies Azure Service Bus messages between queues and topics.

**Entry point:** `Program.cs` — loads configuration, configures Serilog, wires up cancellation (Ctrl+C), creates `MigrationStateManager`, and selects between two modes: namespace migration or per-operation copy.

**Two modes:**

1. **Namespace migration** (preferred for full namespace moves): If `NamespaceMigration` section has valid connection strings, `NamespaceMigrator` auto-discovers all queues and topic subscriptions via `ServiceBusAdministrationClient`, validates destination entities exist, then copies all messages (active + DLQ) in parallel using `Parallel.ForEachAsync` with configurable `MaxConcurrency`. Errors per-entity are caught and logged without stopping other operations.
2. **Per-operation copy** (fallback): If no namespace migration config, falls back to `ServiceBusCopy` section with manually-defined operations run sequentially. Destination validation skips operations where the target entity doesn't exist.

**Core classes:**
- `NamespaceMigrator` — discovers entities, validates destinations, builds `ServiceBusCopyOperation` list, runs them in parallel. Skips entities with zero messages or missing destinations.
- `MessageCopier` — for each source entity, peeks messages from both the main queue and DLQ in sequence, sending copies to the destination. Supports resume from a previous sequence number via `MigrationStateManager`. Implements `IAsyncDisposable`.
- `DestinationValidator` — wraps `ServiceBusAdministrationClient` to check queue/topic existence before attempting copy.
- `MigrationStateManager` — thread-safe state file manager for recovery/resume. Tracks per-entity progress (sequence numbers, message counts, status). Atomic writes via temp file + rename. Disabled in dry-run mode.

**Models:**
- `NamespaceMigrationConfig` — `SourceConnectionString`, `DestinationConnectionString`, `BatchSize` (default: 100), `MaxConcurrency` (default: 5)
- `ServiceBusCopyConfig` — holds a list of `ServiceBusCopyOperation`
- `ServiceBusCopyOperation` — `BatchSize` (default: 100), `Source` and `Destination` endpoints
- `ServiceBusEndpoint` — `ConnectionString`, `QueueName?`, `TopicName?`, `SubscriptionName?`. An endpoint targets a queue (if `QueueName` set) or a topic (if `TopicName` set). Topic sources require `SubscriptionName`.
- `MigrationState` / `EntityMigrationState` / `SubQueueState` — state file models for recovery/resume

No dependency injection container — `ILogger<T>` instances created via `LoggerFactory.Create()` with Serilog.

## Configuration

- `appsettings.json` — committed template with placeholder values
- `appsettings.local.json` — git-ignored, contains real connection strings (overrides appsettings.json)
- Config files are copied to output directory via .csproj `<Content>` items with `PreserveNewest`
- `StateFilePath` — path to the migration state file (default: `migration-state.json`)
- `FreshRun` — set to `true` to delete existing state file and start fresh
- `Serilog` section — log level configuration (default: Information, Azure SDK: Warning)

## Logging

Uses Serilog with two sinks:
- **Console** — `[HH:mm:ss LVL] message` format
- **File** — `logs/migration-YYYYMMDD.log` with daily rolling, full timestamps

## Recovery/Resume

The tool persists progress to a JSON state file (`migration-state.json`) after each batch send. On re-run, it skips completed entities and resumes in-progress ones from the last successful sequence number. Set `"FreshRun": true` to ignore previous state.

## Dependencies

- `Azure.Messaging.ServiceBus` — Azure SDK for peek/send operations and `ServiceBusAdministrationClient` for entity discovery and destination validation
- `Microsoft.Extensions.Configuration.*` — JSON config loading and binding
- `Serilog.*` — structured logging (Console sink, File sink, Configuration integration)
