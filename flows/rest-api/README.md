# rest-api

Openflow (NiFi) flows that call a paginated REST API and stream the records into Snowflake with
Snowpipe Streaming. Committed from runtime `gen2rt-100` via the `GitHubFlowRegistryClient`.

| Flow | What it is |
|---|---|
| `rest-api-to-snowflake-template` | Reusable template: one copy per API, all settings from a parameter context (`PAGE`, `OFFSET`, `NEXT_URL` pagination), failures alerted to Slack |
| `rest-api-to-snowflake-usgs-earthquakes` | Copy of the template configured for the USGS earthquake feed (OFFSET pagination) |

## Parameter contexts expected

Each copy binds an `api_<name>` context that inherits:

- `snowflake_common` — `Snowflake Database`, `Snowflake Schema`, `Snowflake Table`, `Snowflake Role`
- `alerting_common` — `Slack Channel`, `Slack Bot Token` (sensitive)

`api_<name>` holds: `Source Name`, `API Base URL`, `API Key` (sensitive), `Pagination Type`,
`Page Param`, `Size Param`, `Page Size`, `Start Page`, `Records JsonPath`, `Next URL JsonPath`,
`Max Pages`, `Schedule`.

Sensitive values are never committed — set them after importing.

## Importing a new API copy

1. Canvas → drag a Process Group → **Import from Registry** → bucket `rest-api` →
   `rest-api-to-snowflake-template`.
2. Bind (or create) an `api_<name>` context that inherits `snowflake_common` and `alerting_common`.
3. Make sure the API host is allowed by the runtime's External Access Integration.
4. Run `1 Trigger` once, check the target table, then start `1 Trigger` for the schedule.

Full documentation lives with the build scripts: `implementation_guide.md` (Part 1) and
`rest-api-pagination-template-guide.md`.
