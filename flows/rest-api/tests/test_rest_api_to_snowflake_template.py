# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Validation tests for the REST API -> Snowflake (Template) flow.

Structural tests validate the flow definition JSON without a running NiFi instance. The flow is
modular: processors live in child process groups wired through In / Out / Errors ports.
Runtime tests run only when SNOWFLAKE_RUNTIME_URL, SNOWFLAKE_RUNTIME_PAT and DEPLOYED_PG_ID are set.
"""

import json
import os
import re
import time

import pytest


FLOW_PATH = os.path.join(os.path.dirname(__file__), "..", "rest-api-to-snowflake-template.json")

FLOW_NAME = "REST API -> Snowflake (Template)"
API_CONTEXT = "api_pokeapi"
SHARED_CONTEXTS = ["snowflake_common", "alerting_common"]

STD = "org.apache.nifi.processors.standard."
UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
PUBLISH_SLACK = "org.apache.nifi.processors.slack.PublishSlack"
PUT_SNOWPIPE = "com.snowflake.openflow.runtime.processors.snowpipe.PutSnowpipeStreaming"

SLACK = "11 Alert: Slack"
ERRORS = "Errors (inspect queue)"

MODULES = {
    "1 Start Run": {"processors": {"1 Trigger", "2 Init"}, "in": [], "out": ["Out"]},
    "2 Fetch Pages": {"processors": {"3 Build URL", "4 Call API", "8 Read Paging Info", "9 More Pages?",
                                     "10a Next Page", "10b Next Offset", "10c Follow Next URL", "Retry HTTP"},
                      "in": ["In"], "out": ["Errors", "Out"]},
    "3 Load to Snowflake": {"processors": {"5 Split Records", "6 Wrap as Row", "7a Batch", "7b Load to Snowflake"},
                            "in": ["In"], "out": ["Errors"]},
    "4 Alerting": {"processors": {SLACK, ERRORS}, "in": ["In"], "out": []},
}
PORT_TYPES = {"INPUT_PORT", "OUTPUT_PORT"}

PARAM_REF = re.compile(r"#\{([^}]+)\}")


@pytest.fixture
def flow_data():
    with open(FLOW_PATH, "r") as f:
        return json.load(f)


@pytest.fixture
def flow_contents(flow_data):
    return flow_data["flowContents"]


def _groups(group):
    """The group and all nested process groups."""
    yield group
    for child in group.get("processGroups", []):
        yield from _groups(child)


@pytest.fixture
def modules(flow_contents):
    return {g["name"]: g for g in flow_contents.get("processGroups", [])}


@pytest.fixture
def processors(flow_contents):
    return [p for g in _groups(flow_contents) for p in g.get("processors", [])]


@pytest.fixture
def connections(flow_contents):
    return [c for g in _groups(flow_contents) for c in g.get("connections", [])]


@pytest.fixture
def controller_services(flow_contents):
    return flow_contents.get("controllerServices", [])


@pytest.fixture
def parameter_contexts(flow_data):
    return flow_data.get("parameterContexts", {})


@pytest.fixture
def by_name(processors):
    return {p["name"]: p for p in processors}


def _all_parameters(parameter_contexts, context_name):
    """Parameters visible to a context, including inherited ones."""
    ctx = parameter_contexts[context_name]
    params = {p["name"]: p for p in ctx.get("parameters", [])}
    for parent in ctx.get("inheritedParameterContexts", []):
        for name, p in _all_parameters(parameter_contexts, parent).items():
            params.setdefault(name, p)
    return params


def _outgoing(connections, proc_id):
    return [c for c in connections if c["source"]["id"] == proc_id]


def _next_processors(connections, conn):
    """Processor names a connection delivers to, following ports across module boundaries."""
    found, todo, seen = set(), [conn["destination"]], set()
    while todo:
        dest = todo.pop()
        if dest["type"] not in PORT_TYPES:
            found.add(dest["name"])
            continue
        if dest["id"] in seen:
            continue
        seen.add(dest["id"])
        todo.extend(c["destination"] for c in connections if c["source"]["id"] == dest["id"])
    return found


class TestFlowStructure:
    def test_flow_is_valid_json(self, flow_data):
        assert flow_data is not None

    def test_has_flow_contents(self, flow_data):
        assert "flowContents" in flow_data

    def test_flow_name(self, flow_contents):
        assert flow_contents["name"] == FLOW_NAME

    def test_parameter_context_name(self, flow_contents):
        assert flow_contents.get("parameterContextName") == API_CONTEXT

    def test_no_processors_at_top_level(self, flow_contents):
        assert not flow_contents.get("processors"), "Processors belong in module process groups"


class TestModules:
    def test_expected_modules(self, modules):
        assert set(modules) == set(MODULES)

    @pytest.mark.parametrize("module", sorted(MODULES))
    def test_module_processors(self, modules, module):
        names = {p["name"] for p in modules[module].get("processors", [])}
        assert names == MODULES[module]["processors"]

    @pytest.mark.parametrize("module", sorted(MODULES))
    def test_module_ports(self, modules, module):
        group = modules[module]
        assert sorted(p["name"] for p in group.get("inputPorts", [])) == MODULES[module]["in"]
        assert sorted(p["name"] for p in group.get("outputPorts", [])) == MODULES[module]["out"]

    @pytest.mark.parametrize("module", sorted(MODULES))
    def test_module_bound_to_api_context(self, modules, module):
        # Parameter contexts do not cascade to child groups; each module must be bound explicitly.
        assert modules[module].get("parameterContextName") == API_CONTEXT

    @pytest.mark.parametrize("src_module,src_port,dst_module,dst_port", [
        ("1 Start Run", "Out", "2 Fetch Pages", "In"),
        ("2 Fetch Pages", "Out", "3 Load to Snowflake", "In"),
        ("2 Fetch Pages", "Errors", "4 Alerting", "In"),
        ("3 Load to Snowflake", "Errors", "4 Alerting", "In"),
    ])
    def test_module_wiring(self, flow_contents, modules, src_module, src_port, dst_module, dst_port):
        src = {p["name"]: p["identifier"] for p in modules[src_module].get("outputPorts", [])}[src_port]
        dst = {p["name"]: p["identifier"] for p in modules[dst_module].get("inputPorts", [])}[dst_port]
        assert any(c["source"]["id"] == src and c["destination"]["id"] == dst
                   for c in flow_contents.get("connections", [])), \
            f"Missing wiring {src_module}.{src_port} -> {dst_module}.{dst_port}"

    def test_pagination_loop_stays_in_fetch_pages(self, modules):
        fetch = modules["2 Fetch Pages"]
        ids = {p["identifier"] for p in fetch.get("processors", [])}
        loop_edges = {"10a Next Page": "success", "10b Next Offset": "success",
                      "10c Follow Next URL": "success", "Retry HTTP": "retry"}
        for c in fetch.get("connections", []):
            if loop_edges.get(c["source"]["name"]) in c.get("selectedRelationships", []):
                assert c["destination"]["id"] in ids, f"Loop edge from {c['source']['name']} leaves Fetch Pages"

    def test_controller_services_on_parent(self, flow_contents, modules):
        # Shared services live on the parent so every module can use them.
        assert flow_contents.get("controllerServices"), "Parent should hold the shared controller services"
        for name, group in modules.items():
            assert not group.get("controllerServices"), f"{name} should not define its own services"


class TestProcessors:
    EXPECTED_PROCESSORS = [
        ("1 Trigger", STD + "GenerateFlowFile"),
        ("2 Init", UPDATE_ATTRIBUTE),
        ("3 Build URL", UPDATE_ATTRIBUTE),
        ("4 Call API", STD + "InvokeHTTP"),
        ("5 Split Records", STD + "SplitJson"),
        ("6 Wrap as Row", STD + "ReplaceText"),
        ("7a Batch", STD + "MergeRecord"),
        ("7b Load to Snowflake", PUT_SNOWPIPE),
        ("8 Read Paging Info", STD + "EvaluateJsonPath"),
        ("9 More Pages?", STD + "RouteOnAttribute"),
        ("10a Next Page", UPDATE_ATTRIBUTE),
        ("10b Next Offset", UPDATE_ATTRIBUTE),
        ("10c Follow Next URL", UPDATE_ATTRIBUTE),
        ("Retry HTTP", STD + "RetryFlowFile"),
        (SLACK, PUBLISH_SLACK),
        (ERRORS, STD + "LogAttribute"),
    ]

    @pytest.mark.parametrize("name,proc_type", EXPECTED_PROCESSORS)
    def test_expected_processor_exists(self, by_name, name, proc_type):
        assert name in by_name, f"Processor '{name}' not found"
        assert by_name[name]["type"] == proc_type

    def test_all_processors_have_required_fields(self, processors):
        for proc in processors:
            for field in ("identifier", "name", "type", "bundle"):
                assert field in proc, f"Processor {proc.get('name')} missing '{field}'"

    def test_trigger_uses_schedule_parameter(self, by_name):
        assert by_name["1 Trigger"]["schedulingPeriod"] == "#{Schedule}"

    def test_init_copies_parameters_compared_in_el(self, by_name):
        # Parameters inside quoted EL string literals are not substituted, so Init copies the
        # ones that EL functions compare against into attributes.
        props = by_name["2 Init"]["properties"]
        assert props.get("base_url") == "#{API Base URL}"
        assert props.get("page_size") == "#{Page Size}"
        assert props.get("page") == "#{Start Page}"
        assert props.get("pages_fetched") == "0"

    def test_build_url_uses_base_url_attribute(self, by_name):
        url = by_name["3 Build URL"]["properties"]["url"]
        assert url.startswith("${base_url}")
        assert "contains('?')" in url, "Build URL should join with '&' when the base URL has a query"
        assert "${page}" in url

    def test_call_api_uses_url_attribute(self, by_name):
        props = by_name["4 Call API"]["properties"]
        assert props["HTTP Method"] == "GET"
        assert props["HTTP URL"] == "${url}"

    def test_api_key_header_is_sensitive_parameter(self, by_name):
        proc = by_name["4 Call API"]
        assert proc["properties"].get("X-API-Key") == "#{API Key}"
        assert proc["propertyDescriptors"]["X-API-Key"].get("sensitive") is True

    def test_records_path_is_parameterized(self, by_name):
        assert by_name["5 Split Records"]["properties"]["JsonPath Expression"] == "#{Records JsonPath}"

    def test_wrap_as_row_builds_landing_columns(self, by_name):
        value = by_name["6 Wrap as Row"]["properties"]["Replacement Value"]
        for column in ('"SOURCE"', '"PAGE_NUM"', '"REQUEST_URL"', '"INGESTED_AT"', '"RAW":$1'):
            assert column in value, f"Wrap as Row missing {column}"

    def test_ingested_at_is_iso8601(self, by_name):
        value = by_name["6 Wrap as Row"]["properties"]["Replacement Value"]
        assert "yyyy-MM-dd'T'HH:mm:ss.SSS" in value, (
            "INGESTED_AT must be ISO-8601; Snowpipe Streaming rejects space-separated timestamps"
        )

    def test_snowpipe_uses_managed_auth_and_parameters(self, by_name):
        props = by_name["7b Load to Snowflake"]["properties"]
        assert props["Authentication Strategy"] == "SNOWFLAKE_MANAGED"
        assert props["Database"] == "#{Snowflake Database}"
        assert props["Schema"] == "#{Snowflake Schema}"
        assert props["Table"] == "#{Snowflake Table}"
        assert props["Role"] == "#{Snowflake Role}"

    def test_paging_reads_count_and_next(self, by_name):
        props = by_name["8 Read Paging Info"]["properties"]
        assert props["rec_count"] == "#{Records JsonPath}.length()"
        assert props["next_url"] == "#{Next URL JsonPath}"

    @pytest.mark.parametrize("route", ["more_page", "more_offset", "more_next"])
    def test_route_has_rule_and_max_pages_cap(self, by_name, route):
        rule = by_name["9 More Pages?"]["properties"].get(route, "")
        assert rule, f"More Pages? missing '{route}' rule"
        assert "#{Max Pages}" in rule, f"'{route}' must be capped by #{{Max Pages}}"

    @pytest.mark.parametrize("route", ["more_page", "more_offset"])
    def test_full_page_check_uses_attribute(self, by_name, route):
        rule = by_name["9 More Pages?"]["properties"][route]
        assert "${rec_count:equals(${page_size})}" in rule

    def test_slack_uses_alerting_parameters(self, by_name):
        props = by_name[SLACK]["properties"]
        assert props["Access Token"] == "#{Slack Bot Token}"
        assert props["Channel"] == "#{Slack Channel}"
        assert props["Publish Strategy"] == "Use 'Message Text' Property"


class TestExpressionLanguage:
    def test_no_parameters_inside_quoted_el_strings(self, processors):
        # '#{X}' inside an EL string literal is passed through literally, not substituted.
        quoted = re.compile(r"""\$\{[^}]*['"]#\{""")
        for proc in processors:
            for key, value in (proc.get("properties") or {}).items():
                if isinstance(value, str):
                    assert not quoted.search(value), (
                        f"{proc['name']}.{key} uses a parameter inside a quoted EL string: {value}"
                    )


class TestConnections:
    def test_all_connections_have_source_and_destination(self, connections):
        for conn in connections:
            assert conn["source"].get("id"), f"Connection {conn.get('identifier')} has no source"
            assert conn["destination"].get("id"), f"Connection {conn.get('identifier')} has no destination"

    def test_no_orphaned_processors(self, processors, connections):
        connected = {c["source"]["id"] for c in connections} | {c["destination"]["id"] for c in connections}
        for proc in processors:
            assert proc["identifier"] in connected, f"Processor '{proc['name']}' is not connected"

    @pytest.mark.parametrize("source,relationship,destination", [
        ("1 Trigger", "success", "2 Init"),
        ("2 Init", "success", "3 Build URL"),
        ("3 Build URL", "success", "4 Call API"),
        ("4 Call API", "Response", "5 Split Records"),
        ("4 Call API", "Response", "8 Read Paging Info"),
        ("4 Call API", "Retry", "Retry HTTP"),
        ("4 Call API", "Failure", "Retry HTTP"),
        ("Retry HTTP", "retry", "4 Call API"),
        ("7a Batch", "merged", "7b Load to Snowflake"),
        ("9 More Pages?", "more_page", "10a Next Page"),
        ("9 More Pages?", "more_offset", "10b Next Offset"),
        ("9 More Pages?", "more_next", "10c Follow Next URL"),
        ("10a Next Page", "success", "3 Build URL"),
        ("10b Next Offset", "success", "3 Build URL"),
        ("10c Follow Next URL", "success", "4 Call API"),
        (SLACK, "success", ERRORS),
    ])
    def test_expected_route(self, connections, source, relationship, destination):
        matches = [c for c in connections
                   if c["source"]["name"] == source and relationship in (c.get("selectedRelationships") or [])
                   and destination in _next_processors(connections, c)]
        assert matches, f"Missing route {source} -[{relationship}]-> {destination}"


class TestFailureHandling:
    FAILURE_RELATIONSHIPS = {"failure", "Failure", "No Retry", "retries_exceeded"}

    def test_failures_are_never_auto_terminated(self, processors):
        for proc in processors:
            if proc["name"] == ERRORS:
                continue
            dropped = self.FAILURE_RELATIONSHIPS & set(proc.get("autoTerminatedRelationships") or [])
            assert not dropped, f"{proc['name']} auto-terminates failure relationship(s) {dropped}"

    def test_failure_relationships_alert_slack(self, processors, connections):
        for proc in processors:
            if proc["name"] in (SLACK, ERRORS):
                continue
            for conn in _outgoing(connections, proc["identifier"]):
                rels = self.FAILURE_RELATIONSHIPS & set(conn["selectedRelationships"])
                if not rels:
                    continue
                reached = _next_processors(connections, conn)
                assert reached and reached <= {SLACK, "Retry HTTP"}, (
                    f"{proc['name']} {sorted(rels)} reaches {sorted(reached)}, expected {SLACK} (or Retry HTTP)"
                )

    def test_module_errors_ports_reach_slack(self, modules, connections):
        for name, group in modules.items():
            for port in group.get("outputPorts", []):
                if port["name"] != "Errors":
                    continue
                outgoing = [c for c in connections if c["source"]["id"] == port["identifier"]]
                assert outgoing, f"{name}.Errors is not connected"
                for c in outgoing:
                    assert _next_processors(connections, c) == {SLACK}, f"{name}.Errors does not reach {SLACK}"

    def test_slack_never_drops_flowfiles(self, connections, by_name):
        slack_id = by_name[SLACK]["identifier"]
        rels = set()
        for conn in _outgoing(connections, slack_id):
            assert conn["destination"]["name"] == ERRORS
            rels |= set(conn["selectedRelationships"])
        assert {"success", "failure", "rate limited"} <= rels


class TestControllerServices:
    def test_json_reader_exists(self, controller_services):
        assert any("JsonTreeReader" in cs["type"] for cs in controller_services)

    def test_json_writer_exists(self, controller_services):
        assert any("JsonRecordSetWriter" in cs["type"] for cs in controller_services)


class TestParameterContexts:
    API_PARAMS = [
        "Source Name", "API Base URL", "API Key", "Pagination Type", "Page Param", "Size Param",
        "Page Size", "Start Page", "Records JsonPath", "Next URL JsonPath", "Max Pages", "Schedule",
    ]
    SENSITIVE_PARAMS = ["API Key", "Slack Bot Token"]

    def test_contexts_exist(self, parameter_contexts):
        for name in [API_CONTEXT] + SHARED_CONTEXTS:
            assert name in parameter_contexts, f"Parameter context '{name}' missing"

    def test_api_context_inherits_shared_contexts(self, parameter_contexts):
        inherited = parameter_contexts[API_CONTEXT].get("inheritedParameterContexts", [])
        for name in SHARED_CONTEXTS:
            assert name in inherited, f"{API_CONTEXT} should inherit {name}"

    @pytest.mark.parametrize("param_name", API_PARAMS)
    def test_api_parameter_exists(self, parameter_contexts, param_name):
        names = [p["name"] for p in parameter_contexts[API_CONTEXT].get("parameters", [])]
        assert param_name in names

    def test_pagination_type_is_supported(self, parameter_contexts):
        params = _all_parameters(parameter_contexts, API_CONTEXT)
        assert params["Pagination Type"]["value"] in ("PAGE", "OFFSET", "NEXT_URL")

    @pytest.mark.parametrize("param_name", SENSITIVE_PARAMS)
    def test_sensitive_parameters_have_no_committed_value(self, parameter_contexts, param_name):
        param = _all_parameters(parameter_contexts, API_CONTEXT)[param_name]
        assert param.get("sensitive") is True, f"{param_name} must be sensitive"
        assert not param.get("value"), f"{param_name} must not have a committed value"

    def test_every_referenced_parameter_is_defined(self, processors, parameter_contexts):
        defined = set(_all_parameters(parameter_contexts, API_CONTEXT))
        for proc in processors:
            values = list((proc.get("properties") or {}).values()) + [proc.get("schedulingPeriod") or ""]
            for value in values:
                for ref in PARAM_REF.findall(value or ""):
                    assert ref in defined, f"{proc['name']} references undefined parameter #{{{ref}}}"

    def test_referenced_parameters_are_not_empty(self, processors, parameter_contexts):
        # NiFi does not substitute empty parameters; the literal '#{Name}' would remain.
        params = _all_parameters(parameter_contexts, API_CONTEXT)
        for proc in processors:
            for value in (proc.get("properties") or {}).values():
                for ref in PARAM_REF.findall(value or ""):
                    p = params[ref]
                    if not p.get("sensitive"):
                        assert p.get("value"), f"Parameter '{ref}' (used by {proc['name']}) is empty"


try:
    import nipyapi
except ImportError:
    nipyapi = None


@pytest.fixture(scope="module")
def nifi_runtime():
    url = os.environ.get("SNOWFLAKE_RUNTIME_URL", "")
    pat = os.environ.get("SNOWFLAKE_RUNTIME_PAT", "")
    if not url or not pat:
        pytest.skip("No runtime URL/PAT — skipping runtime tests")
    if nipyapi is None:
        pytest.skip("nipyapi not available")
    api_url = url.rstrip("/")
    if not api_url.endswith("/nifi-api"):
        api_url += "/nifi-api"
    nipyapi.config.nifi_config.host = api_url
    nipyapi.config.nifi_config.api_key["bearerAuth"] = f"Bearer {pat}"
    nipyapi.config.nifi_config.api_client = None
    return nipyapi


@pytest.fixture(scope="module")
def deployed_pg_id():
    pg_id = os.environ.get("DEPLOYED_PG_ID", "")
    if not pg_id:
        pytest.skip("No DEPLOYED_PG_ID — skipping runtime tests")
    return pg_id


@pytest.fixture(scope="module")
def completed_run(nifi_runtime, deployed_pg_id):
    """Enable services and run the flow once.

    The CI deploy script may already have started the whole group (trigger included); in that
    case the trigger has fired on start and no Run Once is needed.
    """
    api = nifi_runtime.nifi.FlowApi()
    api.activate_controller_services(
        id=deployed_pg_id,
        body=nifi_runtime.nifi.ActivateControllerServicesEntity(id=deployed_pg_id, state="ENABLED"),
    )
    time.sleep(5)
    keep_stopped = {"1 Trigger", SLACK, ERRORS}
    trigger = None
    for proc in nifi_runtime.canvas.list_all_processors(deployed_pg_id):
        if proc.component.name == "1 Trigger":
            trigger = proc
        elif proc.component.name not in keep_stopped:
            nifi_runtime.canvas.schedule_processor(proc, True)
    assert trigger is not None, "1 Trigger not found in deployed flow"
    trigger = nifi_runtime.canvas.get_processor(trigger.id, "id")
    if trigger.component.state != "RUNNING":
        nifi_runtime.nifi.ProcessorsApi().update_run_status4(
            nifi_runtime.nifi.ProcessorRunStatusEntity(state="RUN_ONCE", revision=trigger.revision), trigger.id)
    time.sleep(90)
    return deployed_pg_id


def _processor(nifi_runtime, pg_id, name):
    matches = [p for p in nifi_runtime.canvas.list_all_processors(pg_id) if p.component.name == name]
    assert matches, f"Processor '{name}' not found"
    return matches[0]


class TestRuntimeExecution:
    def test_api_was_called(self, nifi_runtime, completed_run):
        snap = _processor(nifi_runtime, completed_run, "4 Call API").status.aggregate_snapshot
        assert snap.flow_files_in >= 1, "Call API received no requests"

    def test_records_were_loaded(self, nifi_runtime, completed_run):
        snap = _processor(nifi_runtime, completed_run, "7b Load to Snowflake").status.aggregate_snapshot
        assert snap.flow_files_in >= 1, "Load to Snowflake received no batches"

    def test_nothing_reached_failure_paths(self, nifi_runtime, completed_run):
        slack_id = _processor(nifi_runtime, completed_run, SLACK).id
        queued = sum(c.status.aggregate_snapshot.flow_files_queued
                     for c in nifi_runtime.canvas.list_all_connections(completed_run)
                     if c.component.destination.id == slack_id)
        assert queued == 0, f"{queued} FlowFile(s) queued for Slack alerts"

    def test_no_error_bulletins(self, nifi_runtime, completed_run):
        board = nifi_runtime.nifi.FlowApi().get_bulletin_board(group_id=completed_run)
        errors = [b for b in (board.bulletin_board.bulletins or [])
                  if b.bulletin and b.bulletin.level == "ERROR"]
        assert not errors, "; ".join(f"{b.bulletin.source_name}: {b.bulletin.message}" for b in errors[:5])
