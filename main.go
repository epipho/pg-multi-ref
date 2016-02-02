package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func runSql(db *sql.DB, stmt string) {
	_, err := db.Exec(stmt)
	if err != nil {
		log.Fatal(err)
	}
}

func setupTables(db *sql.DB) {
	runSql(db, "CREATE TABLE IF NOT EXISTS test (id integer, s text)")
	runSql(db, "DELETE FROM test")
	runSql(db, "INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")

	runSql(db, "CREATE TABLE IF NOT EXISTS test2 (a text, b text, c text)")
	runSql(db, "DELETE FROM test2")
	runSql(db, "INSERT INTO test2 VALUES ('a', 'b', 'c'), ('d', 'e', 'f'), ('g', 'h', 'i')")
}

func setupFunctions(db *sql.DB) {
	runSql(db, proc1)
}

func testRef1(db *sql.DB) {
	// refcursors are scoped to the transaction so
	// we must turn off autocommit
	txn, _ := db.Begin()
	row := txn.QueryRow("SELECT a, b FROM rc_2()")

	var a, b string
	// Scan for cursor names
	err := row.Scan(&a, &b)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Got ref cursor: %s and %s", a, b)
	// Fetch cursor 1. Can't use parameters here.
	refrows1, err := txn.Query(fmt.Sprintf("FETCH ALL FROM \"%s\"", a))
	if err != nil {
		log.Fatalf("Error querying %s: %v", a, err)
	}
	// Must consume the rows object before fetching again or the 2nd fetch
	// with throw pq: unexpected DataRow in simple query execution. A packet
	// capture shows that the call is completely valid so it must be something
	// inside pq itself. Closing the rows object to discard it also works.
	for refrows1.Next() {
		var id int
		var s string
		err = refrows1.Scan(&id, &s)
		if err != nil {
			log.Fatalf("Error scanning %s: %v", a, err)
		}
		log.Printf("Row: %d, %s", id, s)
	}
	// Consume 2nd ref cursor as before
	refrows2, err := txn.Query(fmt.Sprintf("FETCH ALL FROM \"%s\"", b))
	if err != nil {
		log.Fatalf("Error querying %s: %v", b, err)
	}
	// can actually commit before consuming here as fetch all has returned all the data into memory
	// and we do not need the rows object.
	// Not sure what would happen if FETCH ALL returned an ungodly number of rows
	for refrows2.Next() {
		var ra, rb, rc string
		err = refrows2.Scan(&ra, &rb, &rc)
		if err != nil {
			log.Fatalf("Error scanning %s: %v", b, err)
		}
		log.Printf("Row: %s, %s, %s", ra, rb, rc)
	}

	// Commit!
	txn.Commit()

}

func main() {
	db, err := sql.Open("postgres", "postgres://postgres:password@172.17.8.101/test?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	setupTables(db)
	setupFunctions(db)

	testRef1(db)
}

// returns 2 ref cursors of two different types
var proc1 = ` 
create or replace function rc_2()
returns TABLE(a refcursor, b refcursor)
as
$$
begin
   open a for select * from test;
   open b for select * from test2;
   return next;
end;
$$
language 'plpgsql';
`
