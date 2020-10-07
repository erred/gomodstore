package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	IndexURL = "https://index.golang.org/index"
	ProxyURL = "https://proxy.golang.org"
)

type IndexRecord struct {
	Path      string `json:"Path"`
	Version   string `json:"Version"`
	Timestamp string `json:"Timestamp"`
	// Timestamp time.Time `json:"Timestamp"`
}

type Counters struct {
	modules  uint64
	err410   uint64
	errOther uint64

	bytesUncompressed uint64
	bytesCompressed   uint64
	bytesDeduped      uint64
}

func main() {
	var baseDir string
	var sleep time.Duration
	var limit int
	flag.StringVar(&baseDir, "dir", "/data", "datastore path")
	flag.DurationVar(&sleep, "interval", time.Minute, "time to sleep when no work")
	flag.IntVar(&limit, "limit", 10, "concurrency limit")
	flag.Parse()

	s := NewStore(baseDir)

	tsb, _ := ioutil.ReadFile(filepath.Join(baseDir, "progress"))
	ts := string(tsb)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sig
		cancel()
	}()

	var counters Counters
	go func() {
		for range time.NewTicker(time.Minute).C {
			fmt.Printf("progress: modules=%d err410=%d errOther=%d uncompressed=%d compressed=%d deduped=%d",
				atomic.LoadUint64(&counters.modules),
				atomic.LoadUint64(&counters.err410),
				atomic.LoadUint64(&counters.errOther),
				atomic.LoadUint64(&counters.bytesUncompressed),
				atomic.LoadUint64(&counters.bytesCompressed),
				atomic.LoadUint64(&counters.bytesDeduped),
			)
		}
	}()

	current := 2000
	for {
		if current < 2000 {
			log.Println("sleeping for ", sleep)
			select {
			case <-time.NewTimer(sleep).C:
			case <-ctx.Done():
				return
			}
		}

		current = 0

		u := IndexURL
		if ts != "" {
			u += "?since=" + ts
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			log.Println("create req: ", u, err)
			continue
		}

		res, err := http.DefaultClient.Do(r)
		if err != nil {
			log.Println("do request: ", err)
			continue
		}

		sem := make(chan struct{}, limit)
		d := json.NewDecoder(res.Body)
		for d.More() {
			var ir IndexRecord
			err = d.Decode(&ir)
			if err != nil {
				break
			}

			// send somewhere
			select {
			case sem <- struct{}{}:
				go func() {
					download(ctx, s, ir, &counters)
					<-sem
				}()
			case <-ctx.Done():
				return
			}

			current++

			ts = ir.Timestamp
			ioutil.WriteFile(filepath.Join(baseDir, "progress"), []byte(ts), 0o644)
		}
		res.Body.Close()
		if err != nil {
			log.Println("decode json: ", err)
			continue
		}
	}
}

func download(ctx context.Context, s *Store, ir IndexRecord, counters *Counters) {
	var bytesCompressed, bytesUncompressed uint64
	defer func() {
		atomic.AddUint64(&counters.modules, 1)
		atomic.AddUint64(&counters.bytesCompressed, bytesCompressed)
		atomic.AddUint64(&counters.bytesUncompressed, bytesUncompressed)
	}()

	zipURL := fmt.Sprintf("%s/%s/@v/%s.zip", ProxyURL, ir.Path, ir.Version)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, zipURL, nil)
	if err != nil {
		log.Println("download request", zipURL, err)
		return
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("downlaod GET ", zipURL, err)
		return
	}
	if res.StatusCode != 200 {
		if res.StatusCode == 410 {
			atomic.AddUint64(&counters.err410, 1)
			return
		}
		atomic.AddUint64(&counters.errOther, 1)
		log.Println("download ", res.StatusCode, res.Status, zipURL)
		return
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println("read ", zipURL, err)
		return
	}
	br := bytes.NewReader(b)
	zr, err := zip.NewReader(br, int64(len(b)))
	if err != nil {
		log.Println("zipreader ", zipURL, err)
		return
	}

	for _, zf := range zr.File {
		bytesCompressed += zf.CompressedSize64
		bytesUncompressed += zf.UncompressedSize64

		rc, err := zf.Open()
		if err != nil {
			log.Println("open zipfile ", zipURL, zf.Name, err)
			continue
		}

		b, err := ioutil.ReadAll(rc)
		rc.Close()
		if err != nil {
			log.Println("read zipfile ", zipURL, zf.Name, err)
			continue
		}
		err = s.Add(counters, zf.Name, b)
		if err != nil {
			log.Println("write zipfile ", zipURL, zf.Name, err)
			continue
		}
	}
}
