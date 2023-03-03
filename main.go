package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"time"
)

type statsT struct {
	CurrentOpenConns  int
	CurrentInUseConns int
	CurrentIdleConn   int
	SelectsFired      int
	SelectsCompleted  int
	SelectsRunning    int
	UnixTime          int
}

var db *sql.DB
var mysqlMaxOpenConns = 5000
var maxOpenConns = 50
var maxIdleConns = 10
var connMaxLifetime = 3
var connMaxIdleTime = 1
var selectsToRun = 100
var selectsFired = 0
var selectsCompleted = 0
var selectSleepSec = 3
var rampUpIntervalMillisec = 150 // time to wait before running the next selects
var rampUpSelects = 25           //number of selects to run every ramp up interval
var dbUser = "root"
var dbPassword = "root"
var dbHost = "127.0.0.1"
var dbPort = "3306"
var selectString string
var wg sync.WaitGroup
var mysqlCommandFilterKeyword = "mysqlpooltesting"
var mx sync.Mutex
var statsCollectionMs int
var stats statsT
var statsHistory struct {
	MysqlMaxOpenConns         int
	MaxOpenConns              int
	MaxIdleConns              int
	ConnMaxLifetime           int
	ConnMaxIdleTime           int
	SelectsToRun              int
	SelectSleep               int
	RampUpIntervalMs          int
	RampUpSelects             int
	MysqlCommandFilterkeyword string
	Stats                     []statsT
}

func main() {
	//TODO PASS VALUES WE WANT TO CHANGE WITH COMMAND AND RETRIEVE THEM HERE BEFORE STARTING

	connect()
	defer closeConnection()

	pushConfigValues()
	initialiseStatsHistory()

	wgAdd("farm")
	selectFarm()
	wg.Wait()
}

func initialiseStatsHistory() {
	statsHistory.MysqlMaxOpenConns = mysqlMaxOpenConns
	statsHistory.MaxOpenConns = maxOpenConns
	statsHistory.MaxIdleConns = maxIdleConns
	statsHistory.ConnMaxLifetime = connMaxLifetime
	statsHistory.ConnMaxIdleTime = connMaxIdleTime
	statsHistory.SelectsToRun = selectsToRun
	statsHistory.SelectSleep = selectSleepSec
	statsHistory.RampUpIntervalMs = rampUpIntervalMillisec
	statsHistory.RampUpSelects = rampUpSelects
	statsHistory.MysqlCommandFilterkeyword = mysqlCommandFilterKeyword
}

func pushConfigValues() {
	setMaxOpenConns(maxOpenConns)
	setMaxIdleConns(maxIdleConns)
	setConnMaxLifetime(connMaxLifetime)
	setConnMaxIdleTime(connMaxIdleTime)

	setMysqlMaxOpenConns(mysqlMaxOpenConns)
	selectString = fmt.Sprintf("select sleep(%d); -- %s", selectSleepSec, mysqlCommandFilterKeyword)
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
	wg.Done()
}
func wgAdd(who string) {
	wg.Add(1)
}

func selectRun(sequence int) {
	defer wgDone(fmt.Sprintf("%d", sequence))
	_, err := db.Exec(selectString)
	if err != nil {
		panic(err)
	}
	mx.Lock()
	selectsCompleted++
	mx.Unlock()

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

func getCurrentOpenConns() int {
	return db.Stats().OpenConnections
}

func getCurrentIdleConns() int {
	return db.Stats().Idle
}

func getCurrentInUseConns() int {
	return db.Stats().InUse
}
