/*
Copyright 2026 The littlered Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"io"
	"strings"
	"testing"
)

// TestRunOneShotGatherRejectsBadTarget pins the target-parse guard: an invalid
// --gather target must error before any cluster access (the parse happens ahead
// of the client build), so a malformed Job command fails fast and clearly.
func TestRunOneShotGatherRejectsBadTarget(t *testing.T) {
	for _, target := range []string{"", "noslash", "/name", "ns/"} {
		err := RunOneShotGather(context.Background(), nil, nil, target, io.Discard)
		if err == nil {
			t.Errorf("target %q: expected an error, got nil", target)
			continue
		}
		if !strings.Contains(err.Error(), "invalid --gather target") {
			t.Errorf("target %q: expected invalid-target error, got %v", target, err)
		}
	}
}
