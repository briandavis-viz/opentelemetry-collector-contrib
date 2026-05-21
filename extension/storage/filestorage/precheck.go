// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

// errDBCorruption is returned by runPrecheck when bbolt's Tx.Check consistency
// walk reports the database as corrupt. The caller is expected to treat this
// as a signal to rename the file aside and create a fresh one.
var errDBCorruption = errors.New("filestorage: bbolt database appears corrupt")

// runPrecheck opens the bbolt database at path in read-only mode and runs the
// public (*bbolt.Tx).Check consistency walk. It returns:
//
//   - nil if the file does not exist or the check passes,
//   - errDBCorruption (wrapped) if Tx.Check reports any consistency error
//     (the "freepages: multiple references" goroutine panic is converted into
//     a channel error by bbolt's own recover wrapper), or
//   - a non-corruption error for anything else (e.g. bbolt.Open failure, file
//     lock timeout, ctx cancellation). The caller proceeds with the normal
//     open in that case so a transient probe failure cannot destroy a healthy
//     database.
//
// Read-only Open skips bbolt.loadFreelist (and therefore the
// goroutine that previously crashed the collector); Tx.Check then walks the
// bucket tree under bbolt's recover() wrapper, so panics raised inside that
// walk arrive back here as regular Go errors on the channel.
func runPrecheck(ctx context.Context, path string, bboltTimeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		ReadOnly: true,
		Timeout:  bboltTimeout,
	})
	if err != nil {
		return fmt.Errorf("precheck: open read-only: %w", err)
	}
	defer func() { _ = db.Close() }()

	return db.View(func(tx *bbolt.Tx) error {
		var firstErr error
		for chErr := range tx.Check() {
			if firstErr == nil && chErr != nil {
				firstErr = chErr
			}
		}
		if firstErr != nil {
			return fmt.Errorf("%w: %v", errDBCorruption, firstErr)
		}
		return nil
	})
}
