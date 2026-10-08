# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.22] 2026-10-08

### Added

- `Twin.torouter(topic, *data, **kwargs)`: publish a message directly
  into this twin's own router inbox for in-process delivery to local
  subscribers, bypassing `publish`'s `isbroker() and isopen()` gate.
  `publish` only takes its in-process delivery path when at least one
  *other* twin is currently connected to the same router (`isopen()`);
  a `ReplTwin` acting purely as an injected "hub" `ctx` for other
  in-process twins (e.g. a main application component referenced via
  `Twin.inject` by several independently-connected MQTT/WS twins, each
  on its *own* separate router, as in a multi-edge gateway topology)
  can never satisfy that condition even though its own locally
  registered subscribers are ready to receive messages — `publish`
  would always raise `RembusConnectionClosed` in that shape. `torouter`
  unconditionally takes the in-process path instead, matching a
  previously-removed method of the same name/behavior.

### Fixed

- `Router._task_impl` processed every message (pub/sub delivery, RPC,
  admin, etc.) through a single sequential inbox, and the periodic
  `data_at_rest` archiver write (`rdb.save_data_at_rest`, a synchronous
  DuckDB/DuckLake write of cached messages) ran inline on that same
  loop. Under real traffic the write can take multiple seconds,
  queuing up and delaying delivery of unrelated messages (e.g. a
  pending RPC/pub-sub response) behind it. The periodic save now runs
  in a background thread (`asyncio.to_thread`), with `msg_cache`/
  `msg_topic_cache` atomically swapped for fresh containers before the
  write starts so the main loop keeps accumulating new messages
  without racing the background write; at most one save runs at a
  time, and a graceful shutdown still awaits any in-flight save before
  closing the database connection.
- Backgrounding that archiver write surfaced two related DuckLake/
  DuckDB concurrency bugs, now also fixed:
  - `Twin._shutdown()` could call `save_twin()` on `self.db` while a
    background archiver write for the *same* router was still mid-write
    on that same connection object (DuckDB connections aren't safe for
    concurrent use from multiple threads); it now awaits any in-flight
    `router._data_at_rest_task` first. The background write itself now
    uses a dedicated `router.db.cursor()` (with `USE rl` re-applied,
    since a fresh cursor doesn't inherit the parent connection's
    default-schema setting) rather than the raw shared connection, so
    it can safely run concurrently with other queries on `router.db`
    from the event-loop thread.
  - Multiple independent `rembus` components within the same process
    (e.g. several `gateway_mqtt`-style components, each with its own
    DuckDB connection) commonly attach the same on-disk DuckLake
    catalog; concurrent writes from two such components (e.g. two
    components shutting down together) could collide with persistent
    `database is locked` errors. All DuckLake write/sync touchpoints
    (`save_data_at_rest`, `sync_twin`) now go through a new
    `with_lock_retry` helper that (a) serializes same-process writers
    via a process-wide lock and (b) retries with capped exponential
    backoff on any residual `database is locked` error from a
    genuinely separate OS process.

## [0.8.21] 2026-10-07

### Fixed

- Fix in-process pubsub delivery of a single non-list payload

### Added

- Added mesh network topology discovery: a connecting node advertises, via
  the `meta` field of the Identity/Attestation handshake, the protocols and
  ports it listens on. The upstream broker collects this information into
  `router.network`, a list of `rembus.core.Node`, enabling brokers to be
  chained into a mesh network.

## [0.8.20] 2026-10-02

- `REMBUS_MQTT_TOPIC_FILTER` now accepts a comma separated list of topic
  filters to subscribe to.

-  Pub/Sub messages are only delivered to reactive subscribers with same tenant
   as the publisher.

- Broker-to-subscriber fan-out (`_broadcast`) now performs a full Ack/Ack2
  handshake with retries for QOS1/QOS2 messages, mirroring the
  publisher-to-broker hop, instead of a fire-and-forget send. QOS0 delivery
  is unaffected.

## [0.8.19] 2026-09-29

### Fixed

- Implement reactive gate.

- Add `application/json` property to MQTT publish message.

## [0.8.18] 2026-09-18

### Changed

- Calling sync node() will always create a fresh instance.

## [0.8.17] 2026-09-10

### Fixed

- Shutdown all routers in the chain.

### Changed

- The full exception stack trace is logged when an RPC handler raises.

## [0.8.16] 2026-09-02

### Changed

- Renamed env REMBUS_MQTT_BASE_TOPIC to REMBUS_MQTT_TOPIC_FILTER.

## [0.8.15] 2026-08-28

### Fixed

- Fix publish dispatcher in case of multiple subscribers.

## [0.8.14] 2026-08-26

### Fixed

- mqtt close logic.

## [0.8.13] 2026-08-12

### Fixed

- Pass the repl node to the rpc callbacks bound to a broker.

## [0.8.12] 2026-08-07

### Fixed

- Improve shutdown logic.

- Fix cleanup when handling OS signals.

## [0.8.11] 2026-08-01

### Added

- Add db BOOLEAN type

### Fixed

- mqtt client component subscribe

## [0.8.10] 2026-06-30

### Added

- Add expose_upsert_table, expose_query_table and expose_delete_table

### Fixed

- Manage SQL tables with special chars.

## [0.8.9] 2026-06-15

### Fixed

- Improve start anyway logic.

## [0.8.8] 2026-05-28

### Changed

- Use orjson for JSON

- MQTT workers

## [0.8.7] 2026-05-14

### Added

- DuckDB automatic migration on connect for version update.

### Fixed

- Send publish messages from subscribe callbacks.

- Fix test_retroactive test.

## [0.8.6] 2026-04-22

### Added

- node `thread` option for marimo integration

- New `cols` column selector option for`query_<topic>`

### Changed

- duckdb compat version.

### Fixed

- Mqtt message loopback.

## [0.8.5] 2026-03-24

### Added

- `upsert_<topic>` rpc service.

- node internal registry

## [0.8.4] 2026-03-21

### Fixed

- Builtins tests

- Upgrade to duckdb 1.5.0

- Fix swdistribution builtins

## [0.8.3] 2026-03-10

### Changed

- Default port 8338 (was the widely used port 8000)

- Add environment variable REMBUS_START_ANYWAY. If true start the
  component even if the broker/server is down.

## [0.8.2] 2026-03-04

### Added

- brokerd entry point script

### Fixed

- Code distribution on components using direct.

## [0.8.1] 2026-02-13

### Fixed

- Docs review and ci workflow.

## [0.8.0] 2026-02-09

### Add

- Code distribution impl started.

- `rembus.anonym()` api for anonymous component creation.

- Auth apis: `authorize`, `private_topic`, `public_topic`.

### Fixed

- Manage space topics subscribed by broker/server components.

## [0.7.2] 2026-01-12

### Fixed

- Fix add_plugin.

## [0.7.1] 2026-01-11

### Added

- Time travel via `when` key added to `query_*` topics dict payload.  

### Fixed

- Restore topics spaces at startup.

## [0.7.0] 2026-01-07

### Fixed

- [Missing $HOME/.config/rembus](https://github.com/cardo-org/rembus.python/issues/1)


