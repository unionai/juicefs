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

package cmd

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

func cmdSliceRefs() *cli.Command {
	return &cli.Command{
		Name:      "slice-refs",
		Action:    sliceRefs,
		Category:  "TOOL",
		Usage:     "List every slice ID referenced by a metadata store",
		ArgsUsage: "META-URL [OUTPUT]",
		Description: `
Walks an (unmounted) metadata store and emits the distinct slice IDs it
references, sorted ascending, one decimal ID per line, to OUTPUT (or
stdout). This is the reference-extraction primitive for the external GC
reaper: union the outputs across all retained commit indexes of a volume
family, and any object in a closed writer-session domain whose slice ID is
absent from the union is garbage.

Runs offline against a commit index (a downloaded sqlite snapshot, or a
badger directory materialized with checkpoint-restore) — never against a
store a live client has open.

Examples:
$ juicefs slice-refs sqlite3://index.db refs.txt
$ juicefs slice-refs badger:///var/lib/vol/fork-meta`,
	}
}

func sliceRefs(ctx *cli.Context) error {
	setup0(ctx, 1, 2)
	removePassword(ctx.Args().Get(0))
	metaConf := meta.DefaultConf()
	metaConf.NoBGJob = true
	m := meta.NewClient(ctx.Args().Get(0), metaConf)
	if _, err := m.Load(true); err != nil {
		return fmt.Errorf("load setting: %s", err)
	}

	slices := make(map[meta.Ino][]meta.Slice)
	if st := m.ListSlices(meta.Background(), slices, false, false, nil); st != 0 {
		return fmt.Errorf("list slices: %s", st)
	}
	seen := make(map[uint64]struct{})
	for _, ss := range slices {
		for _, s := range ss {
			if s.Id > 0 {
				seen[s.Id] = struct{}{}
			}
		}
	}
	ids := make([]uint64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := os.Stdout
	if dst := ctx.Args().Get(1); dst != "" {
		f, err := os.Create(dst)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	w := bufio.NewWriter(out)
	for _, id := range ids {
		w.WriteString(strconv.FormatUint(id, 10)) //nolint:errcheck
		w.WriteByte('\n')                         //nolint:errcheck
	}
	if err := w.Flush(); err != nil {
		return err
	}
	logger.Infof("%d distinct slice IDs referenced", len(ids))
	return nil
}
