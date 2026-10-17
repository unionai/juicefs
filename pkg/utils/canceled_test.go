/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
)

func TestIsCanceled(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{context.Canceled, true},
		{fmt.Errorf("get chunk: %w", context.Canceled), true},
		{&url.Error{Op: "Get", URL: "https://bucket/chunk", Err: context.Canceled}, true},
		{errors.New("read tcp: context canceled"), true},
		{context.DeadlineExceeded, false},
		{errors.New("NoSuchKey: object not found"), false},
	}
	for _, c := range cases {
		if got := IsCanceled(c.err); got != c.want {
			t.Errorf("IsCanceled(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
