package main

import (
	"context"
	"database/sql"
	"encoding/json"
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
var selectsToRun = 1000
var selectsFired = 0
var selectsCompleted = 0
var selectSleepSec = 3
var rampUpIntervalMillisec = 150 // time to wait before running the next selects
var rampUpSelects = 100          //number of selects to run every ramp up interval
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
var StatsHistory struct {
	Config struct {
		MysqlMaxOpenConns         int
		MaxOpenConns              int
		MaxIdleConns              int
		ConnMaxLifetime           int
		ConnMaxIdleTime           int
		SelectsToRun              int
		SelectSleep               int
		RampUpIntervalMs          int
		RampUpSelects             int
		MysqlCommandFilterKeyword string
	}
	Stats []statsT
}

func main() {
	//TODO PASS VALUES WE WANT TO CHANGE WITH COMMAND AND RETRIEVE THEM HERE BEFORE STARTING
	ctx, ctxCancelFn := context.WithCancel(context.Background())
	connect()
	defer closeConnection()

	pushConfigValues()
	initialiseStatsHistory()

	go collectStatsLooper(ctx)
	selectsCannon()
	fmt.Println("All selects fired... now waiting...")
	wg.Wait()

	fmt.Println("Ending the world...")

	//RECYCLE THE WAIT GROUP FOR ANOTHER TASK!
	wgAdd("ctxcancel")
	ctxCancelFn()
	wg.Wait()

	//todo print stats.....
	jsonBytes, err := json.Marshal(StatsHistory)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", string(jsonBytes))

}

func collectStatsLooper(ctx context.Context) {
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			fmt.Println("stats...")
		case <-ctx.Done():
			fmt.Println("cancelled....")
			wg.Done()
			return
		} //end select
	} //end for loop
}

func initialiseStatsHistory() {
	StatsHistory.Config.MysqlMaxOpenConns = mysqlMaxOpenConns
	StatsHistory.Config.MaxOpenConns = maxOpenConns
	StatsHistory.Config.MaxIdleConns = maxIdleConns
	StatsHistory.Config.ConnMaxLifetime = connMaxLifetime
	StatsHistory.Config.ConnMaxIdleTime = connMaxIdleTime
	StatsHistory.Config.SelectsToRun = selectsToRun
	StatsHistory.Config.SelectSleep = selectSleepSec
	StatsHistory.Config.RampUpIntervalMs = rampUpIntervalMillisec
	StatsHistory.Config.RampUpSelects = rampUpSelects
	StatsHistory.Config.MysqlCommandFilterKeyword = mysqlCommandFilterKeyword
}

func pushConfigValues() {
	setMaxOpenConns(maxOpenConns)
	setMaxIdleConns(maxIdleConns)
	setConnMaxLifetime(connMaxLifetime)
	setConnMaxIdleTime(connMaxIdleTime)

	setMysqlMaxOpenConns(mysqlMaxOpenConns)
	selectString = fmt.Sprintf("select sleep(%d); -- %s", selectSleepSec, mysqlCommandFilterKeyword)
}

func selectsCannon() {
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
