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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

func cmdCheckpoint() *cli.Command {
	return &cli.Command{
		Name:      "checkpoint",
		Action:    checkpoint,
		Category:  "TOOL",
		Usage:     "Produce a durable, consistent snapshot of a metadata store",
		ArgsUsage: "MOUNTPOINT|META-URL DST",
		Description: `
With a MOUNTPOINT, asks the running client to (1) flush all buffered
writes, (2) write an engine-native consistent snapshot of the metadata
store to DST (a local path on the machine running the client), and (3)
wait until every block the snapshot references has been uploaded to object
storage. Blocks written after the snapshot are not waited for, so a busy
writer cannot keep the drain going. Every chunk the snapshot references is
durable when this command returns 0 — DST can then be published as a
branch/commit index.

As soon as the snapshot's content is fixed, before it is written out and
before the drain, the command prints "snapshot pinned" on stdout: from then
on nothing written to the file system can change what DST contains, so a
caller holding writers back for the snapshot (a frozen file system) can let
them go.

With a META-URL, snapshots an UNMOUNTED store directly (no drain — there
is no client, so nothing can be staged). This is how directory-shaped
stores (BadgerDB) are snapshotted after unmount, where no live client
holds the store open.

Supported stores: SQLite (VACUUM INTO), Redis (BGSAVE, co-located server),
BadgerDB (store archive: a tar of a consistent, compacted copy of the store
directory, which 'juicefs checkpoint-restore' untars rather than replays).
Other engines return ENOTSUP.

Examples:
$ juicefs checkpoint /mnt/jfs /var/lib/vol/checkpoint.db
$ juicefs checkpoint badger:///var/lib/vol/meta /var/lib/vol/checkpoint.bak`,
		Flags: []cli.Flag{
			&cli.UintFlag{
				Name:  "drain-timeout",
				Value: 600,
				Usage: "seconds to wait for the writeback staging queue to drain",
			},
			&cli.BoolFlag{
				Name:  "delta",
				Usage: "write only what changed since the last checkpoint confirmed with 'juicefs checkpoint-confirm' (BadgerDB); fails with \"no delta base\" when there is none",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "print the result as JSON on stdout: {\"kind\", \"base\", \"read_ts\"}; read_ts is what to confirm once DST is published",
			},
		},
	}
}

func cmdCheckpointConfirm() *cli.Command {
	return &cli.Command{
		Name:      "checkpoint-confirm",
		Action:    checkpointConfirm,
		Category:  "TOOL",
		Usage:     "Tell a mount that one of its checkpoints was published",
		ArgsUsage: "MOUNTPOINT READ_TS",
		Description: `
Makes the checkpoint with READ_TS (from 'juicefs checkpoint --json') the base
that later 'juicefs checkpoint --delta' calls are taken against. Confirm only
checkpoints that were actually published as the next version of the volume
mounted here: a delta is only meaningful on top of what it was taken against.

Examples:
$ juicefs checkpoint --json /mnt/jfs /tmp/c1.tar    # {"kind":"full","read_ts":1042}
$ juicefs checkpoint-confirm /mnt/jfs 1042
$ juicefs checkpoint --delta --json /mnt/jfs /tmp/d1 # {"kind":"delta","base":1042,"read_ts":1307}`,
	}
}

func checkpointConfirm(ctx *cli.Context) error {
	setup(ctx, 2)
	mp := ctx.Args().Get(0)
	ts, err := strconv.ParseUint(ctx.Args().Get(1), 10, 64)
	if err != nil {
		return fmt.Errorf("READ_TS %q: %s", ctx.Args().Get(1), err)
	}
	f, err := openController(mp)
	if err != nil {
		return fmt.Errorf("open control file for %s: %s", mp, err)
	}
	defer f.Close()
	wb := utils.NewBuffer(8 + 8)
	wb.Put32(meta.CheckpointConfirm)
	wb.Put32(8)
	wb.Put64(ts)
	if _, err = f.Write(wb.Bytes()); err != nil {
		return fmt.Errorf("write message: %s", err)
	}
	if _, errno := readProgress(f, func(uint64, uint64) {}); errno != 0 {
		return fmt.Errorf("confirm checkpoint %d on %s: %s", ts, mp, errno)
	}
	return nil
}

func checkpoint(ctx *cli.Context) error {
	setup0(ctx, 2, 2)
	mp := ctx.Args().Get(0)
	dst, err := filepath.Abs(ctx.Args().Get(1))
	if err != nil {
		return fmt.Errorf("abs of %q: %s", ctx.Args().Get(1), err)
	}
	if strings.Contains(mp, "://") {
		// Offline: snapshot an unmounted store directly. No drain — with no
		// running client, nothing can be in the writeback staging queue.
		m := meta.NewClient(mp, nil)
		if err := m.CheckpointStore(meta.Background(), dst); err != nil {
			return fmt.Errorf("checkpoint %s -> %s: %s", mp, dst, err)
		}
		if err := m.Shutdown(); err != nil {
			return fmt.Errorf("close store after checkpoint: %s", err)
		}
		logger.Infof("checkpoint written to %s (offline store)", dst)
		return nil
	}
	f, err := openController(mp)
	if err != nil {
		return fmt.Errorf("open control file for %s: %s", mp, err)
	}
	defer f.Close()

	var flags uint8
	if ctx.Bool("delta") {
		flags |= 1
	}
	if ctx.Bool("json") || ctx.Bool("delta") {
		flags |= 2
	}
	body := uint32(4 + 4 + len(dst))
	if flags != 0 {
		body++ // older clients read no flags byte, so send it only when needed
	}
	wb := utils.NewBuffer(8 + body)
	wb.Put32(meta.Checkpoint)
	wb.Put32(body)
	wb.Put32(uint32(ctx.Uint("drain-timeout")))
	wb.Put32(uint32(len(dst)))
	wb.Put([]byte(dst))
	if flags != 0 {
		wb.Put8(flags)
	}
	if _, err = f.Write(wb.Bytes()); err != nil {
		logger.Fatalf("write message: %s", err)
	}
	progress := utils.NewProgress(false)
	spin := progress.AddCountSpinner("Staged blocks pending")
	announced := false
	data, errno := readProgress(f, func(count, pinned uint64) {
		spin.SetCurrent(int64(count))
		if pinned != 0 && !announced {
			announced = true
			fmt.Println("snapshot pinned")
			_ = os.Stdout.Sync()
		}
	})
	if errno == syscall.ENOENT && ctx.Bool("delta") {
		logger.Fatalf("checkpoint %s -> %s: no delta base (no confirmed checkpoint on this mount); take a full checkpoint", mp, dst)
	}
	if errno != 0 {
		logger.Fatalf("checkpoint %s -> %s: %s", mp, dst, errno)
	}
	progress.Done()
	if flags&2 != 0 {
		if len(data) == 0 {
			logger.Fatalf("checkpoint %s -> %s: the client returned no result (is it older than this command?)", mp, dst)
		}
		fmt.Println(string(data))
	}
	logger.Infof("checkpoint written to %s (writeback drained)", dst)
	return nil
}
