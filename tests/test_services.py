import pytest

from scripts.check_services import parse_containers, readiness_errors


def test_unstarted_stack_is_not_ready():
    assert readiness_errors({"postgres": {}}, []) == ["postgres: missing container"]


@pytest.mark.parametrize("state,health", [("exited", ""), ("running", "unhealthy"), ("running", "starting")])
def test_unavailable_service_is_not_ready(state, health):
    assert readiness_errors(
        {"postgres": {"healthcheck": {"test": ["CMD", "true"]}}},
        [{"Service": "postgres", "State": state, "Health": health}],
    )


def test_init_failure_blocks_readiness():
    services = {"airflow-init": {}}
    failed = [{"Service": "airflow-init", "State": "exited", "ExitCode": 1}]
    assert readiness_errors(services, failed)
    failed[0]["ExitCode"] = 0
    assert not readiness_errors(services, failed)


def test_optional_service_checked_when_present():
    services = {"superset": {"profiles": ["bi"]}}
    assert not readiness_errors(services, [])
    assert readiness_errors(services, [{"Service": "superset", "State": "exited"}])
    assert not readiness_errors(services, [{"Service": "superset", "State": "running"}])


def test_compose_json_formats():
    row = '{"Service": "postgres", "State": "running"}'
    assert parse_containers(row + "\n") == parse_containers("[" + row + "]")
    assert parse_containers("") == []
