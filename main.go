package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	pgx "github.com/jackc/pgx/v4"
)

var (
	path  = flag.String("path", ".", "path containing multiple sql files to run")
	file  = flag.String("file", ".", "path to one sql file to run")
	dburl = flag.String("database", "", "database url")
)

func walkPath(path string, conn *pgx.Conn) error {
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			blocks, err := extractBlocks(f)
			if err != nil {
				return fmt.Errorf("error extracting statement blocks: %v", err)
			}
			if err = rundb(blocks, conn); err != nil {
				return fmt.Errorf("error running sql file %s: %v", f.Name(), err)
			}
		}
		return nil
	})
	return nil
}

func runFile(filename string, conn *pgx.Conn) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	blocks, err := extractBlocks(f)
	if err != nil {
		return fmt.Errorf("error extracting statement blocks: %v", err)
	}

	if err = rundb(blocks, conn); err != nil {
		return fmt.Errorf("error running sql file %s: %v", f.Name(), err)
	}

	return nil
}

func extractBlocks(f *os.File) ([][]byte, error) {
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	splitted := bytes.Split(data, []byte(";"))
	return splitted, nil
}

func rundb(blocks [][]byte, conn *pgx.Conn) error {
	b := &pgx.Batch{}
	for _, block := range blocks {
		b.Queue(string(block))
	}

	var err error
	results := conn.SendBatch(context.Background(), b)

	for i := 0; i < b.Len(); i++ {
		if _, err = results.Exec(); err != nil {
			return fmt.Errorf("error executing statement %s: %v", string(blocks[i]), err)
		}
	}

	return nil
}

func getConn(url string) (*pgx.Conn, error) {
	return pgx.Connect(context.Background(), url)
}

func main() {
	flag.Parse()

	if *path == "" && *file == "" {
		log.Fatalf("no file or path to files provided")
	}

	if *dburl == "" {
		log.Fatal("no database url specified")
	}

	conn, err := pgx.Connect(context.Background(), *dburl)
	if err != nil {
		log.Fatal("unable to connect to database")
	}

	if *path != "" {
		if err = walkPath(*path, conn); err != nil {
			log.Fatal(err)
		}
		return
	}

	if *file != "" {
		if err = runFile(*file, conn); err != nil {
			log.Fatal(err)
		}
	}

}
