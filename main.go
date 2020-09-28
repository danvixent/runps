package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	pgx "github.com/jackc/pgx/v4"
)

var (
	path  = flag.String("path", ".", "path containing multiple sql files to run")
	file  = flag.String("file", ".", "path to one sql file to run")
	dburl = flag.String("database", "", "database url")
)

func walkPath(root string, conn *pgx.Conn) error {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			now := time.Now()
			blocks, err := extractBlocks(f)
			if err != nil {
				return fmt.Errorf("error extracting statement blocks: %v", err)
			}
			if err = rundb(blocks, conn); err != nil {
				return fmt.Errorf("error running file %s: %v", info.Name(), err)
			}
			fmt.Printf("%s(%v)\n", info.Name(), time.Since(now))
		}
		return nil
	})
	return err
}

func runFile(filename string, conn *pgx.Conn) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	now := time.Now()
	if !strings.HasSuffix(f.Name(), ".sql") {
		return fmt.Errorf("file lacks .sql extension")
	}

	blocks, err := extractBlocks(f)
	if err != nil {
		return fmt.Errorf("error extracting statement blocks: %v", err)
	}

	if err = rundb(blocks, conn); err != nil {
		stat, _ := f.Stat()
		return fmt.Errorf("error running file %s: %v", stat.Name(), err)
	}
	fmt.Printf("%s(%v)\n", f.Name(), time.Since(now))
	return nil
}

func extractBlocks(f *os.File) ([]byte, error) {
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}
	return data, nil
}

func rundb(blocks []byte, conn *pgx.Conn) error {
	if _, err := conn.Exec(context.Background(), string(blocks)); err != nil {
		return err
	}
	return nil
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
