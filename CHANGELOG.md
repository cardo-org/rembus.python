# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking:** a message with a single argument that is a list is published
  to MQTT wrapped in an array (`[[1, 2]]`), so that it is not mistaken for a
  list of arguments. An MQTT payload `[[1, 2]]` is received as a single list
  argument.

- **Breaking:** an empty MQTT payload is delivered as a message without
  arguments instead of being dropped, so a zero-argument message now
  round-trips through the MQTT bridge.

- **Breaking:** the encoding of a WebSocket connection is fixed by its first
  message (binary = CBOR, text = JSON). A later message of the other type
  closes the connection with code 1003 (`WS_CLOSE_UNSUPPORTED`). Previously
  the encoding was switched on every received message.

- The mesh router id stamped in `rmark` is an unsigned 64-bit integer (as in
  Rembus.jl) instead of a 128-bit one. Peers accept both.

- **Breaking:** a list of URLs passed to `component()`/`node()` is an ordered
  failover, not a pool of simultaneous connections: the first URL is the
  primary endpoint, the others are tried in order when a connection attempt
  fails. The component keeps a single connection and the identity of the
  primary URL. The previous behavior (one upstream twin per URL) is removed.

### Fixed

- A broker now publishes the arguments of a successful RPC to the reactive
  subscribers of the same topic (excluding the requestor), as Rembus.jl does.

### Changed

- JSON encoding fails with an error on `NaN` and `±Inf` instead of silently
  emitting `null`.

- **Breaking:** a JSON message whose `id` is a number is no longer accepted:
  ids are decimal strings. The id `"0"` is not valid either. Such a message is undecodable (close
  code 1008).

- **Breaking:** an empty, truncated or undecodable message (CBOR or JSON,
  including `NaN`/`Infinity` tokens or an invalid Rembus packet) closes the
  connection with WebSocket close code 1008. No `Response` is sent and the
  message is not ignored.

- Removed the unused `STS_METHOD_ARGS_ERROR` (41) status constant: it was
  never sent, argument errors are reported as `STS_METHOD_EXCEPTION`.

- Authenticated mode: anonymous components connected from the loopback
  interface are allowed to send commands and are not subject to the
  challenge timeout. Anonymous components connected from other addresses are
  still disconnected.

- **Breaking:** the `pin` field of the `Register` message is now
  `SHA256(CBOR([pin, cid]))` (bytes in CBOR, base64 text in JSON) instead of
  the plaintext pin, so the pin is no longer sent in clear. Peers of a
  previous version cannot register with each other.

- A failed attestation is answered with an error response and the router then
  closes the connection after about 0.5 s (as Rembus.jl), instead of leaving
  it open until the challenge timeout.

### Fixed

- Broker: QOS1/QOS2 delivery to subscribers no longer blocks the router
  dispatch loop. The Ack/Ack2 wait and retries run in background tasks, and
  disconnected subscribers are skipped (they recover missed messages through
  data-at-rest replay).

- Register: the broker rejects a key `type` other than `SIG_RSA` or
  `SIG_ECDSA` and a `pubkey` that is not a plain byte string.

- CBOR frames: unknown trailing elements of a received packet are kept and
  forwarded as elements of the relayed packet instead of being dropped.

- CBOR frames: bits 7-4 of the type byte (reserved bits and flags) are now
  kept for every packet type and re-emitted when a message is relayed,
  instead of being dropped for all types except Pub/Sub.

- Authenticated mode: an anonymous client no longer logs
  `recv unknown msg id` when it receives the router's challenge; the
  challenge is ignored.

## [0.8.23] 2026-10-15

### Fixed

- `component(mqtt=...)` no longer raises `UnboundLocalError` when the
  keyspace plugin is not active (`keyspace=False` or not a server): the
  keyspace-to-MQTT subscription is skipped.
- MQTT bridge: a Pub/Sub message with no arguments is published with a
  0-length payload instead of raising; the MQTT v5 check no longer uses the
  private gmqtt `_connection_state` attribute.

### Changed

- Authenticated mode: an `Identity` received while the router's challenge
  is pending no longer triggers a new challenge. The `Identity` response is
  deferred and carries the outcome of the `Attestation` (no data), or a
  generic error if no `Attestation` arrives within `request_timeout`, as in
  Rembus.jl.

### Added

- `Settings.max_ws_frame` (settings key `max_ws_frame`, default
  `rembus.settings.MAX_WS_FRAME` = 60 MiB): maximum WebSocket frame size
  (per frame, not per reassembled message; larger frames close the
  connection with code 1009). Requires `websockets>=16.0`.
  It replaces the `protocol.WS_FRAME_MAXSIZE` constant.
- `rembus.server(name, port, ...)`: a "server" component, the equivalent
  of Rembus.jl `server`. It accepts connections but, unlike a broker, does
  not relay RPC requests between connected components: a request from an
  ordinary component is resolved only by methods exposed by the server
  itself (otherwise `STS_METHOD_NOT_FOUND`). Direct RPCs to other
  components are rejected with `STS_GENERIC_ERROR` ("direct not permitted in
  server mode"), and published messages are delivered only to the
  server's own subscribers, not broadcast to other components. Requests
  and messages from broker peers are still routed.
- `overwrite_connection` setting (env `REMBUS_OVERWRITE_CONNECTION`,
  default `False`): when enabled, a new connection using the `cid` of an
  already connected component replaces the prior one, which is closed.
- Authenticated mode (`authenticated=True` on `node`/`component`/`server`/
  `connect`, or `connection_mode: "authenticated"` / env
  `REMBUS_CONNECTION_MODE` in settings), as in Rembus.jl. A router in this
  mode sends a challenge to every connecting component and disconnects it
  if it does not authenticate within `challenge_timeout` seconds (default
  3, env `REMBUS_CHALLENGE_TIMEOUT`); pub/sub, RPC, admin and unregister
  commands from an unauthenticated component close its connection.
  Anonymous components are rejected by an authenticated component.

### Changed

- RPC routing selects exposers belonging to the tenant of the requester
  (previously the tenant was derived from the topic name) and the
  `first_up` policy now honours tenants, as in Rembus.jl.
- `register` fails with "tenant [x] not enabled" when the tenant has no
  secret configured.
- A connection attempt with an already connected `cid` is now rejected
  with an "already connected" error response (previously the new twin
  was closed silently). An empty `cid` is rejected with an error too.
- The status code `STS_ERROR` is renamed `STS_GENERIC_ERROR`.
- Only WebSocket clients send RFC 6455 pings (`ws_ping_interval`); the
  server side no longer sends them.

### Fixed

- `tools/tenant.py`: `remove_tenant` SQL syntax error.
- With `overwrite_connection`, a replaced component no longer reconnects
  and evicts its successor: the server closes it with WebSocket close
  code 4000 and the client stays closed. A replaced twin also no longer
  removes its successor from the router or marks it as down.
- `rembus.register` now uses an anonymous connection, so provisioning
  works also against an authenticated router.
- `unregister` is now rejected (`STS_GENERIC_ERROR`, "invalid operation") when the
  requesting component is not authenticated, as in Rembus.jl.
- An unrecognized admin command now gets a `STS_UNKNOWN_ADMIN_CMD`
  response, as in Rembus.jl (previously it silently returned `STS_OK`).

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


