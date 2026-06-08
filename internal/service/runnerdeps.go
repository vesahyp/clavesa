package service

import (
	"github.com/vesahyp/clavesa/internal/runnerreqs"
)

// RunnerRequirements returns the raw content of the workspace's runner
// requirements file (extra Python pip deps installed into the runner image
// for transform UDFs). Empty string when no file exists yet.
func (s *Service) RunnerRequirements() (string, error) {
	return runnerreqs.Read(s.workspace)
}

// SetRunnerRequirements overwrites the runner requirements file with content.
func (s *Service) SetRunnerRequirements(content string) error {
	return runnerreqs.Write(s.workspace, content)
}

// ListRunnerRequirements returns the meaningful requirement lines (blanks and
// comments stripped).
func (s *Service) ListRunnerRequirements() ([]string, error) {
	content, err := runnerreqs.Read(s.workspace)
	if err != nil {
		return nil, err
	}
	return runnerreqs.Lines(content), nil
}

// AddRunnerRequirement appends spec unless a line for the same package name
// already exists. Returns whether a line was added.
func (s *Service) AddRunnerRequirement(spec string) (bool, error) {
	content, err := runnerreqs.Read(s.workspace)
	if err != nil {
		return false, err
	}
	updated, added := runnerreqs.AddLine(content, spec)
	if !added {
		return false, nil
	}
	if err := runnerreqs.Write(s.workspace, updated); err != nil {
		return false, err
	}
	return true, nil
}

// RemoveRunnerRequirement drops any line for spec's package name. Returns
// whether a line was removed.
func (s *Service) RemoveRunnerRequirement(spec string) (bool, error) {
	content, err := runnerreqs.Read(s.workspace)
	if err != nil {
		return false, err
	}
	updated, removed := runnerreqs.RemoveLine(content, spec)
	if !removed {
		return false, nil
	}
	if err := runnerreqs.Write(s.workspace, updated); err != nil {
		return false, err
	}
	return true, nil
}
