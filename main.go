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
	"gopkg.in/cheggaaa/pb.v1"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getRows(db *sql.DB, tableName string) int {
	query, err := db.Prepare("select count(*) from " + tableName)
	check(err)

	defer query.Close()

	var output int
	err = query.QueryRow().Scan(&output)
	if err != nil {

		return -1
	}
	return output
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
	fmt.Println("Convering...")
	readMessages(dbIn, dbOut)

}

func readMessages(db *sql.DB, dbOut *sql.DB) {
	messagesRows := getRows(db, "messages")
	importBar := pb.StartNew(messagesRows)

	rows, err := db.Query("select text from messages")
	currentCount := 0
	check(err)
	defer rows.Close()
	for rows.Next() {
		var text string
		err = rows.Scan(&text)
		check(err)
		for _, Word := range strings.Split(text, " ") {
			Word = stemming(Word)
			if checkWord(dbOut, Word) == -10 {
				insertWord(dbOut, Word)
			}
		}
		currentCount++
		importBar.Increment()
	}
	importBar.Finish()
	wordsRows := getRows(dbOut, "words")
	fmt.Printf("%d rows are processed from Db In. %d words in Db Out.", messagesRows, wordsRows)
	fmt.Println()
	err = rows.Err()
	check(err)
}

func checkWord(db *sql.DB, Word string) int {
	query, err := db.Prepare("select categoryid from words where word = ?")
	check(err)

	defer query.Close()

	var category int
	err = query.QueryRow(Word).Scan(&category)
	if err != nil {
		return -10
	}
	return category
}

func insertWord(db *sql.DB, Word string) {
	tx, err := db.Begin()
	check(err)
	query, err := tx.Prepare("insert into words(word, categoryid, userid) values(?, ?, ?)")
	check(err)
	defer query.Close()

	_, err = query.Exec(Word, -1, 0)
	check(err)
	tx.Commit()
}

func stemming(Word string) string {
	var re = regexp.MustCompile(`[a-z]|[@$%&*~#=/_"!?. ,:;\-\\+1234567890(){}\[\]]`)
	Word = re.ReplaceAllString(Word, "")
	stemmed, err := snowball.Stem(Word, "russian", true)
	check(err)
	return stemmed
}
