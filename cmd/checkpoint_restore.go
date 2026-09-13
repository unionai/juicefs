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
	"io"
	"os"
	"strings"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

func cmdCheckpointRestore() *cli.Command {
	return &cli.Command{
		Name:      "checkpoint-restore",
		Action:    checkpointRestore,
		Category:  "TOOL",
		Usage:     "Materialize a metadata store from a checkpoint artifact",
		ArgsUsage: "SRC META-URL",
		Description: `
Populates a fresh metadata store at META-URL from a checkpoint artifact SRC
produced by 'juicefs checkpoint' on the same engine, so the volume can be
mounted from it (e.g. forking a branch from a published commit index).

The destination must not hold any existing data; restore never merges into
or overwrites a live store.

Engines whose checkpoint artifact is directly usable need no restore step:
SQLite snapshots are themselves mountable database files (this command just
copies them for convenience), and Redis RDB files are loaded by the server.

BadgerDB checkpoints are store archives — a tar of the store directory — and
this command untars one into META-URL's directory; no engine is involved and
nothing is replayed. Artifacts published before that format existed are
Badger backup streams, and are detected and replayed instead.

Examples:
$ juicefs checkpoint-restore /var/lib/vol/checkpoint.bak badger:///var/lib/vol/meta
$ juicefs checkpoint-restore /var/lib/vol/checkpoint.db sqlite3:///var/lib/vol/meta.db`,
	}
}

func checkpointRestore(ctx *cli.Context) error {
	setup0(ctx, 2, 2)
	src := ctx.Args().Get(0)
	uri := ctx.Args().Get(1)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("checkpoint artifact %s: %s", src, err)
	}

	// SQLite checkpoints are complete database files; a copy IS the restore.
	// Doing it here (not via a meta client) avoids the client creating
	// schema in the destination before the copy.
	if p, ok := strings.CutPrefix(uri, "sqlite3://"); ok {
		p = strings.Split(p, "?")[0]
		if _, err := os.Stat(p); err == nil {
			return fmt.Errorf("destination %s already exists; restore requires a fresh path", p)
		}
		in, err := os.Open(src)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(p)
		if err != nil {
			return err
		}
		if _, err = io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		if err = out.Close(); err != nil {
			return err
		}
		logger.Infof("restored %s to %s", src, p)
		return nil
	}

	// A store archive holds the engine's own directory, so the restore is an
	// untar and the engine never has to replay anything. Sniffing the
	// artifact rather than trusting the caller keeps older, dump-shaped
	// checkpoints restorable through the same command.
	archive, err := meta.IsStoreArchive(src)
	if err != nil {
		return fmt.Errorf("read checkpoint artifact %s: %s", src, err)
	}
	if archive {
		dir, ok := strings.CutPrefix(uri, "badger://")
		if !ok {
			return fmt.Errorf("%s is a store archive, which only directory-shaped stores (badger://) can restore; got %s", src, uri)
		}
		dir = strings.Split(dir, "?")[0]
		if err := meta.RestoreStoreArchive(src, dir); err != nil {
			return fmt.Errorf("restore %s into %s: %s", src, uri, err)
		}
		logger.Infof("restored %s to %s (store archive)", src, uri)
		return nil
	}

	m := meta.NewClient(uri, nil)
	if err := m.RestoreStore(meta.Background(), src); err != nil {
		return fmt.Errorf("restore %s into %s: %s", src, uri, err)
	}
	if err := m.Shutdown(); err != nil {
		return fmt.Errorf("close restored store: %s", err)
	}
	logger.Infof("restored %s to %s", src, uri)
	return nil
}
