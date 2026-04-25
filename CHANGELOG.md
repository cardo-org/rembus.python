# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.7]

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


