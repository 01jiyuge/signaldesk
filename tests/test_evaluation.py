import copy
import json
import warnings
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import langsmith
import langsmith.evaluation
import pytest
import requests

from signaldesk import evaluation_cli
from signaldesk.evaluation import (
    evaluate_cases,
    load_cases,
    score_fixture_metrics,
    upload_to_langsmith,
)


async def test_quality_suite_runs_real_application_without_network_even_when_tracing_enabled(
    monkeypatch,
):
    def forbidden(*args, **kwargs):
        raise AssertionError("Offline evaluation must not create clients or send network requests")

    async def forbidden_async(*args, **kwargs):
        forbidden()

    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    monkeypatch.setenv("LANGSMITH_API_KEY", "must-not-upload")
    monkeypatch.setattr(langsmith, "Client", forbidden)
    monkeypatch.setattr(httpx.Client, "send", forbidden)
    monkeypatch.setattr(httpx.AsyncClient, "send", forbidden_async)
    monkeypatch.setattr("signaldesk.research.ChatOpenAI", forbidden)
    report = await evaluate_cases()
    assert report["summary"]["scenarios"] == 9
    assert report["summary"]["passed"] == 9
    assert report["summary"]["checks"] == report["summary"]["checks_passed"]
    assert report["summary"]["remote_model_quality_evaluated"] is False
    assert report["summary"]["metric_groups"]["fixture_facts"] == {
        "expected_checks": 6,
        "matched": 6,
    }
    assert report["summary"]["human_fact_support_rate"] == {
        "status": "not_evaluated",
        "value": None,
    }
    assert report["langsmith"]["uploaded"] is False
    change = next(item for item in report["scenarios"] if item["scenario"] == "change")
    assert change["observed"]["fact_fields"]["price_before"] == 29
    assert change["observed"]["fact_fields"]["price_after"] == 39
    assert all(
        "expected" in item and "observed" in item and "pass" in item for item in change["checks"]
    )


async def test_quality_mismatch_is_recorded_as_failure_instead_of_assumed_pass():
    document = copy.deepcopy(load_cases())
    document["cases"] = [document["cases"][0]]
    document["cases"][0]["expected"]["finding_count"] = 999
    report = await evaluate_cases(document)
    assert report["summary"]["failed"] == 1
    check = next(
        item for item in report["scenarios"][0]["checks"] if item["metric"] == "finding_count"
    )
    assert check == {"metric": "finding_count", "expected": 999, "observed": 2, "pass": False}


async def test_failed_scenario_does_not_copy_raw_exception_into_eval_artifact(monkeypatch):
    async def fail(*args):
        raise RuntimeError("Authorization: Bearer private-provider-key; private source body")

    monkeypatch.setattr("signaldesk.evaluation.observe_case", fail)
    document = copy.deepcopy(load_cases())
    document["cases"] = document["cases"][:1]
    report = await evaluate_cases(document)
    assert report["summary"]["failed"] == 1
    serialized = json.dumps(report)
    assert "private-provider-key" not in serialized
    assert "private source body" not in serialized
    assert report["scenarios"][0]["observed"]["error_type"] == "RuntimeError"


def small_report():
    return {
        "summary": {"passed": 1, "scenarios": 1, "checks_passed": 2, "checks": 2, "failed": 0},
        "scenarios": [
            {
                "id": "test",
                "scenario": "baseline",
                "pass": True,
                "description": "DO_NOT_UPLOAD_SECRET",
                "expected": {
                    "finding_count": 2,
                    "execution_succeeded": True,
                    "source_text": "DO_NOT_UPLOAD_SECRET",
                },
                "observed": {
                    "finding_count": 2,
                    "execution_succeeded": True,
                    "source_text": "DO_NOT_UPLOAD_SECRET",
                },
            }
        ],
        "langsmith": {"uploaded": False, "experiment_url": None},
    }


def test_cli_defaults_to_local_json_and_never_calls_upload(tmp_path, monkeypatch, capsys):
    async def local_results():
        return small_report()

    async def forbidden_upload(*args, **kwargs):
        raise AssertionError("A key alone must not enable uploads")

    monkeypatch.setenv("LANGSMITH_API_KEY", "a-key-is-not-consent")
    monkeypatch.setattr(evaluation_cli, "evaluate_cases", local_results)
    monkeypatch.setattr(evaluation_cli, "upload_to_langsmith", forbidden_upload)
    target = tmp_path / "results" / "quality.json"
    assert evaluation_cli.main(["--output", str(target)]) == 0
    assert json.loads(target.read_text())["langsmith"]["uploaded"] is False
    output = capsys.readouterr()
    assert "1/1" in output.out
    assert "人工事实支持率尚未评测" in output.out
    assert output.err == ""


def test_cli_explicit_upload_requires_key_before_running(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("LANGSMITH_API_KEY", raising=False)
    assert evaluation_cli.main(["--langsmith"]) == 2
    assert "LANGSMITH_API_KEY" in capsys.readouterr().err


async def test_upload_rejects_missing_key_without_creating_a_client(monkeypatch):
    monkeypatch.delenv("LANGSMITH_API_KEY", raising=False)

    def forbidden_client(*args, **kwargs):
        pytest.fail("Missing key must be rejected before constructing a cloud client")

    monkeypatch.setattr(langsmith, "Client", forbidden_client)
    with pytest.raises(ValueError, match="LANGSMITH_API_KEY"):
        await upload_to_langsmith(small_report())


async def test_opt_in_aevaluate_receives_only_safe_metrics_and_returns_sdk_url(monkeypatch):
    captured = {}
    dataset_id = uuid4()

    class LocalClient:
        def __init__(self, **kwargs):
            captured["client_config"] = kwargs

        def create_dataset(self, **kwargs):
            captured["dataset"] = kwargs
            return SimpleNamespace(id=dataset_id)

        def create_examples(self, **kwargs):
            captured["examples"] = kwargs

        def flush(self):
            captured["flushed"] = True

        def close(self):
            captured["closed"] = True

    async def local_aevaluate(target, *, data, evaluators, **kwargs):
        captured["aevaluate"] = kwargs
        assert len(data) == 1
        assert data[0].dataset_id == dataset_id
        observed = await target(data[0].inputs)
        captured["target"] = observed
        score = evaluators[0](observed, data[0].outputs)
        assert score["score"] is True
        return SimpleNamespace(
            url="https://smith.langchain.com/test/fixture-experiment",
            experiment_name="fixture-test",
        )

    monkeypatch.setenv("LANGSMITH_API_KEY", "explicit-test-key")
    monkeypatch.setattr(langsmith, "Client", LocalClient)
    monkeypatch.setattr(langsmith.evaluation, "aevaluate", local_aevaluate)
    result = await upload_to_langsmith(small_report())
    assert result["uploaded"] is True
    assert result["experiment_url"].endswith("/fixture-experiment")
    assert captured["aevaluate"]["upload_results"] is True
    assert captured["aevaluate"]["disable_evaluator_tracing"] is True
    assert captured["client_config"]["omit_traced_runtime_info"] is True
    assert captured["flushed"] and captured["closed"]
    public_payload = json.dumps(
        {key: captured[key] for key in ["dataset", "examples", "target"]}, default=str
    )
    assert "DO_NOT_UPLOAD_SECRET" not in public_payload
    assert "explicit-test-key" not in public_payload


async def test_installed_aevaluate_accepts_same_examples_and_evaluator_without_network(monkeypatch):
    calls = []

    def no_network(*args, **kwargs):
        calls.append("network-attempt")
        raise AssertionError("This SDK contract test must stay local")

    monkeypatch.setattr(requests.Session, "request", no_network)
    # upload_results=False in this SDK can still trace target runs. Intercept
    # those public client operations; default application eval never calls it.
    monkeypatch.setattr(
        langsmith.Client, "create_run", lambda *args, **kwargs: calls.append("create-run")
    )
    monkeypatch.setattr(
        langsmith.Client, "update_run", lambda *args, **kwargs: calls.append("update-run")
    )
    client = langsmith.Client(
        api_url="http://127.0.0.1:19876",
        api_key="local-unused",
        auto_batch_tracing=False,
        info=langsmith.schemas.LangSmithInfo(version="local-fixture"),
    )
    example = langsmith.schemas.Example(
        id=uuid4(),
        dataset_id=uuid4(),
        created_at=datetime.now(UTC),
        inputs={"case_index": 0},
        outputs={"finding_count": 2},
    )

    async def target(inputs):
        return {"passed": True, "metrics": {"finding_count": 2}}

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            results = await langsmith.evaluation.aevaluate(
                target,
                data=[example],
                evaluators=[score_fixture_metrics],
                client=client,
                upload_results=False,
                disable_evaluator_tracing=True,
                blocking=True,
            )
        rows = [row async for row in results]
        assert rows[0]["evaluation_results"]["results"][0].score is True
        assert "network-attempt" not in calls
        assert "create-run" in calls
    finally:
        client.close()


def test_upload_failure_retains_local_report_and_does_not_print_raw_error(
    tmp_path, monkeypatch, capsys
):
    async def local_results():
        return small_report()

    async def failed_upload(*args, **kwargs):
        raise RuntimeError("Authorization: Bearer never-print-this")

    monkeypatch.setenv("LANGSMITH_API_KEY", "test-key")
    monkeypatch.setattr(evaluation_cli, "evaluate_cases", local_results)
    monkeypatch.setattr(evaluation_cli, "upload_to_langsmith", failed_upload)
    target = tmp_path / "failed-upload.json"
    assert evaluation_cli.main(["--langsmith", "--output", str(target)]) == 2
    assert json.loads(target.read_text())["summary"]["failed"] == 0
    output = capsys.readouterr()
    assert "never-print-this" not in output.out + output.err + target.read_text()
    assert "离线结果仍有效" in output.err
