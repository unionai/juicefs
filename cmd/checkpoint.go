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
	"fmt"
	"path/filepath"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

func cmdCheckpoint() *cli.Command {
	return &cli.Command{
		Name:      "checkpoint",
		Action:    checkpoint,
		Category:  "TOOL",
		Usage:     "Produce a durable, consistent snapshot of a live mount's metadata store",
		ArgsUsage: "MOUNTPOINT DST",
		Description: `
Asks the running client to (1) flush all buffered writes, (2) write an
engine-native consistent snapshot of the metadata store to DST (a local
path on the machine running the client), and (3) wait until the writeback
staging queue has fully drained to object storage. The drain runs after the
snapshot, so every chunk the snapshot references is durable when this
command returns 0 — DST can then be published as a branch/commit index.

Supported stores: SQLite (VACUUM INTO), Redis (BGSAVE, co-located server),
BadgerDB (backup stream). Other engines return ENOTSUP.

Examples:
$ juicefs checkpoint /mnt/jfs /var/lib/vol/checkpoint.db`,
		Flags: []cli.Flag{
			&cli.UintFlag{
				Name:  "drain-timeout",
				Value: 600,
				Usage: "seconds to wait for the writeback staging queue to drain",
			},
		},
	}
}

func checkpoint(ctx *cli.Context) error {
	setup0(ctx, 2, 2)
	mp := ctx.Args().Get(0)
	dst, err := filepath.Abs(ctx.Args().Get(1))
	if err != nil {
		return fmt.Errorf("abs of %q: %s", ctx.Args().Get(1), err)
	}
	f, err := openController(mp)
	if err != nil {
		return fmt.Errorf("open control file for %s: %s", mp, err)
	}
	defer f.Close()

	wb := utils.NewBuffer(8 + 4 + 4 + uint32(len(dst)))
	wb.Put32(meta.Checkpoint)
	wb.Put32(4 + 4 + uint32(len(dst)))
	wb.Put32(uint32(ctx.Uint("drain-timeout")))
	wb.Put32(uint32(len(dst)))
	wb.Put([]byte(dst))
	if _, err = f.Write(wb.Bytes()); err != nil {
		logger.Fatalf("write message: %s", err)
	}
	progress := utils.NewProgress(false)
	spin := progress.AddCountSpinner("Staged blocks pending")
	if _, errno := readProgress(f, func(count, bytes uint64) {
		spin.SetCurrent(int64(count))
	}); errno != 0 {
		logger.Fatalf("checkpoint %s -> %s: %s", mp, dst, errno)
	}
	progress.Done()
	logger.Infof("checkpoint written to %s (writeback drained)", dst)
	return nil
}
