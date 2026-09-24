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

package multisite

import (
	"fmt"
	"strings"
)

// ExtractResultJSON pulls the tier-2 gather Result object out of a Job pod's
// log. The pod log merges the operator's stdout (our pretty-printed Result JSON)
// with its stderr (zap / site-client log lines, themselves single-line JSON
// objects), so a plain json.Decode of the whole log would fail on the first
// noise line. This scans for balanced top-level {...} objects — respecting
// string literals and escapes so a brace inside a message is not a false
// boundary — and returns the first that carries the "groundTruth" key (the
// Result discriminator). Returns an error if no such object is present.
func ExtractResultJSON(logText string) (string, error) {
	depth := 0
	start := -1
	inStr := false
	esc := false
	for i := 0; i < len(logText); i++ {
		c := logText[i]
		if inStr {
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			if depth == 0 {
				start = i
			}
			depth++
		case '}':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					obj := logText[start : i+1]
					if strings.Contains(obj, `"groundTruth"`) {
						return obj, nil
					}
					start = -1
				}
			}
		}
	}
	return "", fmt.Errorf("no gather Result object (with a groundTruth key) found in the Job log")
}
