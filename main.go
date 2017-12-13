package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/kljensen/snowball"
	_ "github.com/mattn/go-sqlite3"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	dbInput := flag.String("in", "", "InputDB (SQLite3, see scheme)")
	dbOutput := flag.String("out", "", "OutputDB (SQLite3, see scheme)")

	flag.Parse()

	fromDB := *dbInput
	toDB := *dbOutput

	var err error
	//
	// DBOut Init
	dbDriver := "sqlite3"
	dbIn, err := sql.Open(dbDriver, fromDB)
	check(err)
	if err = dbIn.Ping(); err != nil {
		log.Fatal(err)
	}
	defer dbIn.Close()
	fmt.Println("Db In initialized")

	// DBOut Init
	dbOut, err := sql.Open(dbDriver, toDB)
	check(err)
	if err = dbOut.Ping(); err != nil {
		log.Fatal(err)
	}
	defer dbOut.Close()

	fmt.Println("Db Out initialized")

	// Read file

	readMessages(dbIn, dbOut)

}

func readMessages(db *sql.DB, dbOut *sql.DB) {
	rows, err := db.Query("select text from messages")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var text string
		err = rows.Scan(&text)
		if err != nil {
			log.Fatal(err)
		}

		for _, Word := range strings.Split(text, " ") {

			Word = stemming(Word)
			if checkWord(dbOut, Word) == -1 {
				insertWord(dbOut, Word)
			}

		}

	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func checkWord(db *sql.DB, Word string) int {
	query, err := db.Prepare("select categoryid from words where word = ?")
	check(err)

	defer query.Close()

	var category int
	err = query.QueryRow(Word).Scan(&category)
	if err != nil {

		return -1
	}
	return category
}

func insertWord(db *sql.DB, Word string) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	query, err := tx.Prepare("insert into words(word, categoryid, userid) values(?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()

	_, err = query.Exec(Word, -1, 0)
	if err != nil {
		log.Println(err)
		log.Fatal(err)
	}
	tx.Commit()
}

func stemming(Word string) string {
	var re = regexp.MustCompile(`[a-z]|[@$%&*~#=/_"!?. ,:;\-\\+1234567890(){}\[\]]`)
	Word = re.ReplaceAllString(Word, "")
	stemmed, err := snowball.Stem(Word, "russian", true)
	check(err)
	return stemmed
}
