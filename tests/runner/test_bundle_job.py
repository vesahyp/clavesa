"""Unit tests for the job loop of runner/bundle_task.py (ADR-031 section 4):
run_if, outcomes, retries, timeouts, conditions, for_each. No Spark: the
task handler is a fake. Run with plain python3 (make test-runner-py)."""

import importlib.util
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "bundle_task", os.path.join(HERE, "..", "..", "runner", "bundle_task.py")
)
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)


class RulesTest(unittest.TestCase):
    def test_run_if(self):
        S, F, E = "success", "failed", "excluded"
        cases = [
            ("ALL_SUCCESS", [S, S], True), ("ALL_SUCCESS", [S, F], False), ("ALL_SUCCESS", [S, E], False),
            ("ALL_DONE", [F, E], True),
            ("NONE_FAILED", [S, E], True), ("NONE_FAILED", [S, F], False),
            ("AT_LEAST_ONE_SUCCESS", [F, S], True), ("AT_LEAST_ONE_SUCCESS", [F, E], False),
            ("ALL_FAILED", [F, "timedout"], True), ("ALL_FAILED", [F, S], False),
            ("AT_LEAST_ONE_FAILED", [S, F], True), ("AT_LEAST_ONE_FAILED", [S, E], False),
            ("ALL_SUCCESS", [], True),
        ]
        for run_if, parents, want in cases:
            self.assertEqual(bt.should_run(run_if, parents), want, (run_if, parents))
        with self.assertRaises(bt.TaskError):
            bt.should_run("SOMETIMES", ["success"])

    def test_condition(self):
        self.assertTrue(bt.evaluate_condition("GREATER_THAN", "7", "0"))
        self.assertFalse(bt.evaluate_condition("EQUAL_TO", "12.0", "12"))  # the docs' example
        self.assertTrue(bt.evaluate_condition("EQUAL_TO", True, "true"))
        self.assertTrue(bt.evaluate_condition("LESS_THAN_OR_EQUAL", "2.5", "2.5"))
        with self.assertRaises(bt.TaskError):
            bt.evaluate_condition("GREATER_THAN", "abc", "1")

    def test_outcome(self):
        results = {"check": {"result_state": "success", "outcome": "false"}}
        self.assertEqual(bt.dependency_state({"task_key": "check", "outcome": "true"}, results), "excluded")
        self.assertEqual(bt.dependency_state({"task_key": "check", "outcome": "false"}, results), "success")
        self.assertEqual(bt.dependency_state({"task_key": "check"}, results), "success")


def job_event(tasks, run_id="r1"):
    """A pipeline event with a _bundle_job for the given {key: task} dict."""
    order = list(tasks)
    transforms = [{"node": k, "task": {"task_key": k, "task": t}} for k, t in tasks.items() if not t.get("disabled")]
    return {"transforms": transforms, "_bundle_job": {"order": order, "run_id": run_id, "params": {}}}


class LoopTest(unittest.TestCase):
    def setUp(self):
        bt._STATES.clear()

    def run_with(self, tasks, behave):
        """behave(key, attempt) -> None (ok) or an exception to raise."""
        calls = []

        def handler(sub, _ctx):
            key = sub["_bundle_task"]["task_key"]
            calls.append((key, sub["_bundle_attempt"]))
            exc = behave(key, sub["_bundle_attempt"])
            if exc:
                bt._state_of("r1").results[key] = {"result_state": "failed"}
                raise exc
            return {"status": "ok"}

        resp = bt.run_job(job_event(tasks), None, handler)
        return resp, calls

    def test_failure_and_run_if(self):
        tasks = {
            "a": {},
            "b": {"depends_on": [{"task_key": "a"}]},
            "c": {"depends_on": [{"task_key": "b"}]},
            "cleanup": {"depends_on": [{"task_key": "c"}], "run_if": "ALL_DONE"},
            "alert": {"depends_on": [{"task_key": "b"}], "run_if": "AT_LEAST_ONE_FAILED"},
        }
        resp, calls = self.run_with(tasks, lambda k, n: RuntimeError("boom") if k == "b" else None)
        by = {r["node"]: r["status"] for r in resp["transforms"]}
        self.assertEqual(by, {"a": "ok", "b": "failed", "c": "skipped", "cleanup": "ok", "alert": "ok"})
        self.assertEqual(resp["status"], "failed")
        self.assertEqual(resp["failed_node"], "b")

    def test_retries(self):
        tasks = {"a": {"max_retries": 2, "min_retry_interval_millis": 1}}
        resp, calls = self.run_with(tasks, lambda k, n: RuntimeError("flaky") if n < 3 else None)
        self.assertEqual(calls, [("a", 1), ("a", 2), ("a", 3)])
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["transforms"][0]["attempts"], 3)
        resp, calls = self.run_with({"a": {"max_retries": 1}}, lambda k, n: RuntimeError("always"))
        self.assertEqual(len(calls), 2)
        self.assertEqual(resp["status"], "failed")

    def test_timeout_not_retried_unless_asked(self):
        resp, calls = self.run_with({"a": {"max_retries": 3, "timeout_seconds": 1}},
                                    lambda k, n: bt.TaskTimeout("slow"))
        self.assertEqual(len(calls), 1)
        resp, calls = self.run_with({"a": {"max_retries": 1, "timeout_seconds": 1, "retry_on_timeout": True}},
                                    lambda k, n: bt.TaskTimeout("slow"))
        self.assertEqual(len(calls), 2)

    def test_disabled_parent_does_not_block(self):
        tasks = {"a": {"disabled": True}, "b": {"depends_on": [{"task_key": "a"}]}}
        resp, calls = self.run_with(tasks, lambda k, n: None)
        self.assertEqual(calls, [("b", 1)])


class FakeJobsAPI:
    """Jobs API 2.2 run-now / runs/get: the run ends on the second poll."""

    def __init__(self, results):
        self.results, self.calls, self.polls = results, [], 0

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if path.endswith("/run-now"):
            return {"run_id": 42}
        self.polls += 1
        if self.polls < 2:
            return {"state": {"life_cycle_state": "RUNNING"}, "tasks": []}
        return {"state": {"life_cycle_state": "TERMINATED"},
                "tasks": [{"task_key": k, "state": {"result_state": v}} for k, v in self.results.items()]}


class MixedTest(unittest.TestCase):
    def setUp(self):
        bt._STATES.clear()

    def test_local_databricks_segment(self):
        tasks = {
            "a": {},
            "gold": {"depends_on": [{"task_key": "a"}]},
            "after": {"depends_on": [{"task_key": "gold"}]},
        }
        ev = job_event(tasks)
        ev["_bundle_job"]["databricks_job_id"] = 7
        ev["_bundle_job"]["segments"] = [{"id": 0}, {"id": 1}, {"id": 2}]
        for t in ev["transforms"]:
            if t["node"] == "gold":
                t["task"]["place"], t["task"]["segment"] = "databricks", 1
        api = FakeJobsAPI({"gold": "SUCCESS"})
        saved = bt.DatabricksJobs
        bt.DatabricksJobs = lambda: api
        try:
            orig = bt.run_databricks_segment
            bt.run_databricks_segment = lambda job, seg, states, entries: orig(job, seg, states, entries, api=api, poll_seconds=0)
            ran = []
            resp = bt.run_job(ev, None, lambda sub, ctx: ran.append(sub["_bundle_task"]["task_key"]) or {"status": "ok"})
        finally:
            bt.DatabricksJobs = saved
            bt.run_databricks_segment = orig
        self.assertEqual(ran, ["a", "after"])
        self.assertEqual(api.calls[0], ("POST", "/api/2.2/jobs/run-now", {"job_id": 7, "only": ["gold"]}))
        self.assertEqual(resp["states"]["gold"]["result_state"], "success")
        self.assertEqual(resp["status"], "ok")

    def test_failed_segment_holds_back_children(self):
        tasks = {"gold": {}, "after": {"depends_on": [{"task_key": "gold"}]},
                 "alert": {"depends_on": [{"task_key": "gold"}], "run_if": "AT_LEAST_ONE_FAILED"}}
        ev = job_event(tasks)
        ev["_bundle_job"]["databricks_job_id"] = 7
        ev["_bundle_job"]["segments"] = [{"id": 0}, {"id": 1}]
        ev["transforms"][0]["task"]["place"] = "databricks"
        api = FakeJobsAPI({"gold": "FAILED"})
        orig = bt.run_databricks_segment
        bt.run_databricks_segment = lambda job, seg, states, entries: orig(job, seg, states, entries, api=api, poll_seconds=0)
        try:
            ran = []
            resp = bt.run_job(ev, None, lambda sub, ctx: ran.append(sub["_bundle_task"]["task_key"]) or {"status": "ok"})
        finally:
            bt.run_databricks_segment = orig
        self.assertEqual(ran, ["alert"])
        self.assertEqual(resp["status"], "failed")
        self.assertTrue(resp["failed"])

    def test_states_from_an_earlier_segment(self):
        # The cloud: segment 2 runs in a later Lambda call with the states so far.
        tasks = {"gold": {}, "after": {"depends_on": [{"task_key": "gold"}]}}
        ev = job_event(tasks)
        ev["transforms"] = [t for t in ev["transforms"] if t["node"] == "after"]
        ev["_bundle_job"]["segments"] = [{"id": 0}, {"id": 1}]
        ev["_bundle_states"] = {"gold": {"result_state": "success"}}
        ran = []
        resp = bt.run_job(ev, None, lambda sub, ctx: ran.append(sub["_bundle_task"]["task_key"]) or {"status": "ok"})
        self.assertEqual(ran, ["after"])
        self.assertEqual(set(resp["states"]), {"gold", "after"})

    def test_dbx_status_and_runnable(self):
        api = FakeJobsAPI({"g1": "SUCCESS", "g2": "UPSTREAM_FAILED"})
        api.polls = 5
        done, st = bt.dbx_status(api, 42, ["g1", "g2", "g3"])
        self.assertTrue(done)
        self.assertEqual({k: v["result_state"] for k, v in st.items()},
                         {"g1": "success", "g2": "upstream_failed", "g3": "excluded"})
        job_tasks = {"g1": {"task": {"depends_on": [{"task_key": "a"}]}},
                     "g2": {"task": {"depends_on": [{"task_key": "g1"}]}}}
        self.assertEqual(bt.segment_runnable(job_tasks, ["g1", "g2"], {"a": {"result_state": "failed"}}), ["g2"])
        self.assertEqual(bt.segment_runnable(job_tasks, ["g1", "g2"], {"a": {"result_state": "success"}}), ["g1", "g2"])


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=1).result
    sys.exit(0 if result.wasSuccessful() else 1)
