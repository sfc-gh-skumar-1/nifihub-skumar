# REST API -> Snowflake (Template)

Calls a paginated REST API and streams every record into a Snowflake table with Snowpipe Streaming.
The flow is a **template**: everything API-specific comes from parameters, so one flow definition
serves any API. Deploy one copy per API and bind each copy to its own `api_<name>` parameter context.
Failures on any path are posted to Slack and parked in a queue for inspection.

## Modules

The flow is split into four child process groups (modules), wired through ports on the parent:

```
[1 Start Run] ──Out──► [2 Fetch Pages] ──Out──► [3 Load to Snowflake]
                          │Errors                    │Errors
                          └──────────► [4 Alerting] ◄┘
```

| Module | Processors | Ports |
|---|---|---|
| `1 Start Run` | `1 Trigger`, `2 Init` | Out |
| `2 Fetch Pages` | `3 Build URL`, `4 Call API`, `8 Read Paging Info`, `9 More Pages?`, `10a`/`10b`/`10c`, `Retry HTTP` | In, Out (one FlowFile per page response), Errors |
| `3 Load to Snowflake` | `5 Split Records`, `6 Wrap as Row`, `7a Batch`, `7b Load to Snowflake` | In, Errors |
| `4 Alerting` | `11 Alert: Slack`, `Errors (inspect queue)` | In |

- The whole pagination loop stays inside `2 Fetch Pages`.
- The JSON Reader / Writer controller services live on the parent and are shared by all modules.
- Parameter contexts do **not** cascade to child groups: the parent **and every module** are bound
  to the `api_<name>` context.

## How It Works

1. `1 Trigger` (GenerateFlowFile) starts a run every `#{Schedule}`.
2. `2 Init` stamps the run state as attributes (`page`, `pages_fetched`, `base_url`, `page_size`, ...).
3. `3 Build URL` composes `<API Base URL>?<Size Param>=<Page Size>&<Page Param>=<page>`
   (joins with `&` if the base URL already has a query string).
4. `4 Call API` (InvokeHTTP) performs the GET. The response is cloned into two branches:
   - **Records:** `5 Split Records` (`#{Records JsonPath}`) → `6 Wrap as Row` → `7a Batch` →
     `7b Load to Snowflake` (PutSnowpipeStreaming, `SNOWFLAKE_MANAGED` auth).
   - **Paging:** `8 Read Paging Info` → `9 More Pages?` → `10a`/`10b`/`10c` → loop back.
5. The run ends when a page comes back short (PAGE / OFFSET), there is no next link (NEXT_URL), or
   `#{Max Pages}` is reached.
6. Every failure relationship goes to `11 Alert: Slack` (PublishSlack) and then to
   `Errors (inspect queue)`, which is left stopped so failed FlowFiles can be inspected.

Pagination styles:

| `Pagination Type` | Next request | Last page |
|---|---|---|
| `PAGE` | `page + 1` | fewer than `Page Size` records |
| `OFFSET` | `offset + Page Size` | fewer than `Page Size` records |
| `NEXT_URL` | URL found at `Next URL JsonPath` | next link empty or null |

Each landed row looks like:

```json
{"SOURCE": "...", "PAGE_NUM": 0, "REQUEST_URL": "...", "INGESTED_AT": "2026-11-14T19:42:54.862", "RAW": { ...record... }}
```

## Parameters

The flow binds `api_pokeapi` (shipped as a working PokéAPI example), which inherits two shared
contexts. When deploying for another API, create `api_<name>` with the same parameter names.

**`api_<name>`** (per API)

| Parameter | Description |
|-----------|-------------|
| `Source Name` | Value written to the `SOURCE` column and shown in Slack alerts |
| `API Base URL` | Endpoint; may include fixed query params (e.g. `...?format=geojson`). Only send params the API documents — some APIs reject unknown ones |
| `API Key` | Sent as header `X-API-Key` (sensitive). Use `none` for public APIs |
| `Pagination Type` | `PAGE`, `OFFSET` or `NEXT_URL` |
| `Page Param` / `Size Param` | Query param names for page/offset and page size |
| `Page Size` | Records per page; also the "full page" test |
| `Start Page` | First page or offset (`1` for most PAGE APIs, `0` or `1` for OFFSET) |
| `Records JsonPath` | Path to the record array, e.g. `$.results`, `$.features` |
| `Next URL JsonPath` | Path to the next-page URL (NEXT_URL only), e.g. `$.next` |
| `Max Pages` | Safety cap on requests per run |
| `Schedule` | Run Schedule of `1 Trigger` (e.g. `1 day`) |

**`snowflake_common`** (shared)

| Parameter | Description |
|-----------|-------------|
| `Snowflake Database` / `Snowflake Schema` / `Snowflake Table` | Target table |
| `Snowflake Role` | Role used by PutSnowpipeStreaming (the runtime's execute-as role) |

**`alerting_common`** (shared)

| Parameter | Description |
|-----------|-------------|
| `Slack Channel` | Channel for failure alerts |
| `Slack Bot Token` | Bot token (`xoxb-...`, scope `chat:write`) — sensitive |

Every referenced parameter must have a non-empty value: NiFi leaves an empty parameter as the
literal `#{Name}`.

## Required NARs

None beyond what an Openflow runtime ships: `nifi-standard-nar`, `nifi-update-attribute-nar`,
`nifi-record-serialization-services-nar`, `nifi-slack-nar`, and Openflow's
`runtime-snowpipe-processors-nar` (PutSnowpipeStreaming).

## Configuration

1. **Target table** (example DDL):
   ```sql
   CREATE TABLE IF NOT EXISTS <db>.<schema>.API_LANDING (
       SOURCE STRING, PAGE_NUM NUMBER, REQUEST_URL STRING, INGESTED_AT TIMESTAMP_NTZ, RAW VARIANT);
   GRANT INSERT, SELECT ON TABLE <db>.<schema>.API_LANDING TO ROLE <runtime role>;
   ```
2. **Network access:** the runtime's External Access Integration must allow the API host
   (e.g. `pokeapi.co:443`) and `slack.com:443`.
3. **Slack:** set `Slack Bot Token` / `Slack Channel`, invite the bot to the channel, then start
   `11 Alert: Slack`.
4. **First run:** right-click `1 Trigger` → *Run Once*, check the table, then start `1 Trigger`.

`INGESTED_AT` must be ISO-8601 (`yyyy-MM-dd'T'HH:mm:ss.SSS`); Snowpipe Streaming rejects
space-separated timestamps for `TIMESTAMP_NTZ`.

## Deploying a copy for another API

1. Import from this registry (bucket `rest-api`, flow `rest-api-to-snowflake-template`) and rename
   the process group, e.g. `REST API -> Snowflake (USGS)`.
2. Bind the **parent and each of the four modules** to that API's `api_<name>` context
   (Configure → Parameter Context on each group).
3. Renaming and re-binding are not local modifications, and the bindings survive version upgrades,
   so the copy keeps tracking new template versions (Version → Change version).

## Deployment

Import from this registry (bucket `rest-api`, flow `rest-api-to-snowflake-template`) or deploy via
the nifihub CD pipeline. `tests/test_rest_api_to_snowflake_template.yaml` shows a CI runtime
configuration using the PokéAPI parameters.
