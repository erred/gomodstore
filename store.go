package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

type Store struct {
	baseDir string
	content string
	mod     string
}

func NewStore(baseDir string) *Store {
	s := &Store{
		baseDir: baseDir,
		content: filepath.Join(baseDir, "content"),
		mod:     filepath.Join(baseDir, "mod"),
	}
	return s
}

func (s *Store) Add(counters *Counters, rfpath string, b []byte) error {
	sum := sha256.Sum256(b)
	cspath := filepath.Join(s.content, hex.EncodeToString(sum[:]))

	os.MkdirAll(filepath.Dir(cspath), 0o755)
	f, err := os.Create(cspath)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("store.Add create %s: %w", cspath, err)
	} else if err == nil {
		defer f.Close()
		_, err := f.Write(b)
		if err != nil {
			return fmt.Errorf("store.Add write %s: %w", cspath, err)
		}
		atomic.AddUint64(&counters.bytesDeduped, uint64(len(b)))
	}

	fpath := filepath.Join(s.mod, rfpath)

	os.MkdirAll(filepath.Dir(fpath), 0o755)
	os.Remove(fpath)
	err = os.Link(cspath, fpath)
	if err != nil {
		return fmt.Errorf("store.Add: %w", err)

	}
	return nil
}
