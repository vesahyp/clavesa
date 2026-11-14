package bundle

import (
	"strings"
	"testing"
)

const varsBundle = `
bundle: { name: v }
include: [ "res/*.yml" ]
variables:
  a: { default: from-default }
  b: {}
  c: { default: "${var.a}-c" }
  cx: { type: complex, default: { k: [1, 2] } }
targets:
  t1:
    default: true
    variables: { b: from-target }
  t2:
    variables: { b: x, cx: { other: true } }
`

const varsJob = `
resources:
  jobs:
    j:
      name: "${bundle.name}-${bundle.target}-${var.a}-${var.c}"
      tasks:
        - task_key: k
          notebook_task: { notebook_path: ../nb/x.py, base_parameters: { cx: "${var.cx}" } }
`

func TestResolveVariablesAndPaths(t *testing.T) {
	dir := writeBundle(t, varsBundle, map[string]string{"res/j.yml": varsJob, "nb/x.py": "# Databricks notebook source"})

	b, err := Resolve(dir, Options{Env: noEnv})
	if err != nil {
		t.Fatal(err)
	}
	if b.Target != "t1" {
		t.Errorf("default target: got %s", b.Target)
	}
	if got := b.Jobs["j"]["name"]; got != "v-t1-from-default-from-default-c" {
		t.Errorf("name: %v", got)
	}
	jobs, _ := b.TypedJobs()
	if p := jobs[0].Tasks[0].NotebookTask.NotebookPath; p != "nb/x.py" {
		t.Errorf("notebook path relative to the bundle root: got %s", p)
	}

	// --var beats BUNDLE_VAR_ beats the target.
	env := func(k string) (string, bool) {
		if k == "BUNDLE_VAR_b" {
			return "from-env", true
		}
		return "", false
	}
	b, _ = Resolve(dir, Options{Env: env})
	if b.Variables["b"] != "from-env" {
		t.Errorf("env: %v", b.Variables["b"])
	}
	b, _ = Resolve(dir, Options{Env: env, Vars: map[string]string{"b": "from-flag", "a": "A"}})
	if b.Variables["b"] != "from-flag" || b.Variables["c"] != "A-c" {
		t.Errorf("flag: b=%v c=%v", b.Variables["b"], b.Variables["c"])
	}

	// A target's complex value replaces the default as a whole.
	b, _ = Resolve(dir, Options{Target: "t2", Env: noEnv})
	cx := b.Variables["cx"].(map[string]any)
	if _, ok := cx["k"]; ok || cx["other"] != true {
		t.Errorf("complex replace: %v", cx)
	}
}

func TestResolveErrors(t *testing.T) {
	dir := writeBundle(t, varsBundle, map[string]string{"res/j.yml": varsJob})
	for _, c := range []struct {
		opts Options
		want string
	}{
		{Options{Target: "nope", Env: noEnv}, `no target "nope"`},
		{Options{Env: noEnv, Vars: map[string]string{"zz": "1"}}, "declares no such variable"},
		{Options{Env: noEnv, Vars: map[string]string{"cx": "1"}}, "complex variable cannot be set"},
	} {
		if _, err := Resolve(dir, c.opts); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("want %q, got %v", c.want, err)
		}
	}

	noValue := writeBundle(t, "bundle: { name: n }\nvariables: { b: {} }\ntargets: { t: {} }\n", nil)
	if _, err := Resolve(noValue, Options{Env: noEnv}); err == nil || !strings.Contains(err.Error(), "no value for variable b") {
		t.Errorf("missing value: %v", err)
	}
	dup := writeBundle(t, "bundle: { name: n }\ninclude: [a.yml, b.yml]\ntargets: { t: {} }\n",
		map[string]string{"a.yml": "resources: { jobs: { j: { name: a } } }", "b.yml": "resources: { jobs: { j: { name: b } } }"})
	if _, err := Resolve(dup, Options{Env: noEnv}); err == nil || !strings.Contains(err.Error(), "defined twice") {
		t.Errorf("duplicate job: %v", err)
	}
	two := writeBundle(t, "bundle: { name: n }\ntargets: { a: {}, b: {} }\n", nil)
	if _, err := Resolve(two, Options{Env: noEnv}); err == nil || !strings.Contains(err.Error(), "none is the default") {
		t.Errorf("no default target: %v", err)
	}
}
