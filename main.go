package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"time"
)

var db *sql.DB
var mysqlMaxOpenConns = 5000
var maxOpenConns = 50
var maxIdleConns = 10
var connMaxLifetime = 3
var connMaxIdleTime = 1
var selectsToRun = 1000
var selectsFired = 0
var selectsCompleted = 0
var selectSleepSec = 10
var rampUpIntervalMillisec = 150 // time to wait before running the next selects
var rampUpSelects = 50           //number of selects to run every ramp up interval
var dbUser = "root"
var dbPassword = "root"
var dbHost = "127.0.0.1"
var dbPort = "3306"
var selectString string
var wg sync.WaitGroup

func main() {
	connect()
	defer closeConnection()

	setMaxOpenConns(maxOpenConns)
	setMaxIdleConns(maxIdleConns)
	setConnMaxLifetime(connMaxLifetime)
	setConnMaxIdleTime(connMaxIdleTime)

	setMysqlMaxOpenConns(mysqlMaxOpenConns)
	selectString = fmt.Sprintf("select sleep(%d);", selectSleepSec)
	wgAdd("farm")
	fmt.Println("🙂")
	fmt.Println("Farming...")
	selectFarm()
	fmt.Println("Waiting...")
	fmt.Printf("%v", db.Stats())
	wg.Wait()
	go fmt.Print("HELLO!")
}

func selectFarm() {
	defer wgDone("farm")
	for selectsFired < selectsToRun {
		for i := 0; i < rampUpSelects; i++ {
			selectsFired++
			wgAdd(fmt.Sprintf("%d", selectsFired))
			go selectRun(selectsFired)

		}
		time.Sleep(time.Duration(rampUpIntervalMillisec) * time.Millisecond)
	}
}

func wgDone(who string) {
	fmt.Printf("[%s 😈]", who)

	wg.Done()
}
func wgAdd(who string) {
	fmt.Printf("[%s 🙂]", who)

	wg.Add(1)
}

func selectRun(sequence int) {
	defer wgDone(fmt.Sprintf("%d", sequence))
	_, err := db.Exec(selectString)
	if err != nil {
		panic(err)
	}
}

func closeConnection() {
	db.Close()
}

func connect() {
	var err error
	db, err = sql.Open("mysql", getConnDns())
	if err != nil {
		panic(err)
	}

}

func getConnDns() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)
}

func setMaxOpenConns(v int) {
	maxOpenConns = v
	db.SetMaxOpenConns(v)
}
func getMaxOpenConns() int {
	return maxOpenConns
}

func setMaxIdleConns(v int) {
	maxIdleConns = v
	db.SetMaxIdleConns(v)
}
func getMaxIdleConns() int {
	return maxIdleConns
}

func setConnMaxLifetime(m int) {
	connMaxLifetime = m
	db.SetConnMaxLifetime(time.Minute * time.Duration(m))
}
func getConnMaxLifetime() int {
	return connMaxLifetime
}

func setConnMaxIdleTime(m int) {
	connMaxIdleTime = m
	db.SetConnMaxIdleTime(time.Minute * time.Duration(m))
}
func getConnMaxIdleTime() int {
	return connMaxIdleTime
}
func setMysqlMaxOpenConns(v int) {
	sql := fmt.Sprintf("set global max_connections = %d;", v)
	db.Exec(sql)
}
