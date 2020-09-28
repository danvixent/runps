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
	path  = flag.String("path", "", "path containing multiple sql files to run")
	file  = flag.String("file", "", "path to one sql file to run")
	dburl = flag.String("database", "", "database url")
)

// walkPath runs all .sql files in root against conn.
// the files in their natural order(alphabetical)
func walkPath(root string, conn *pgx.Conn) error {
	// use filepath.Walk to simplify directory traversal
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() { // skip if it's a directory
			return runFile(path, conn)
		}
		return nil
	})
	return err
}

// runFile runs file against conn
func runFile(file string, conn *pgx.Conn) error {
	now := time.Now() // for logging performance
	if !strings.HasSuffix(file, ".sql") {
		return fmt.Errorf("file lacks .sql extension")
	}

	// read all file bytes
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return fmt.Errorf("error reading fileJ: %v", err)
	}

	// extract the file name, eliminiating the absolute path
	file = file[strings.LastIndex(file, string(os.PathSeparator))+1:]

	// exceute the file on conn
	if _, err := conn.Exec(context.Background(), string(bytes)); err != nil {
		return fmt.Errorf("error running file %s: %v", file, err)
	}

	fmt.Printf("%s(%v)\n", file, time.Since(now)) // log time taken to process this file
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

	// open a connection to the database
	conn, err := pgx.Connect(context.Background(), *dburl)
	if err != nil {
		log.Fatal("unable to connect to database")
	}

	// if path flag was set call walkPath and exit
	if *path != "" {
		if err = walkPath(*path, conn); err != nil {
			log.Fatal(err)
		}
		return
	}

	// if the file flag was set call runFile
	if *file != "" {
		if err = runFile(*file, conn); err != nil {
			log.Fatal(err)
		}
	}

}
