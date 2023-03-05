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

type configT struct {
	MysqlMaxOpenConns         int
	MaxOpenConns              int
	MaxIdleConns              int
	ConnMaxLifetime           int
	ConnMaxIdleTime           int
	SelectsToRun              int
	SelectSleepSec            int
	RampUpIntervalMs          int
	RampUpSelects             int
	MysqlCommandFilterKeyword string
	StatsCollectionMs         int
	SelectsBatches            int
}

var config = configT{
	MysqlMaxOpenConns:         5000,
	MaxOpenConns:              50,
	MaxIdleConns:              10,
	ConnMaxLifetime:           3,
	ConnMaxIdleTime:           1,
	SelectsToRun:              1000,
	SelectSleepSec:            2,
	RampUpIntervalMs:          150,
	RampUpSelects:             50,
	MysqlCommandFilterKeyword: "mysqlpooltesting",
	StatsCollectionMs:         150,
}

var db *sql.DB

var selectsFired = 0
var selectsCompleted = 0
var selectsBatchesFired = 0
var dbUser = "root"
var dbPassword = "root"
var dbHost = "127.0.0.1"
var dbPort = "3306"
var selectString string
var wg sync.WaitGroup
var wgSelects sync.WaitGroup
var mx sync.Mutex
var stats statsT
var StatsHistory struct {
	StartTimeUnix int64
	EndTimeUnix   int64
	Stats         []statsT
}

func main() {
	StatsHistory.StartTimeUnix = time.Now().Unix()
	//TODO PASS VALUES WE WANT TO CHANGE WITH COMMAND AND RETRIEVE THEM HERE BEFORE STARTING
	ctx, ctxCancelFn := context.WithCancel(context.Background())
	connect()
	defer closeConnection()

	pushConfigValuesToDbObject()
	calculateSelectBatches()

	wg.Add(1)
	go printProgress(ctx)

	wg.Add(1)
	go collectStatsLooper(ctx)

	selectsCannon()
	wgSelects.Wait()
	StatsHistory.EndTimeUnix = time.Now().Unix()

	//give time to the progress to update
	//before cancelling the context
	time.Sleep(500 * time.Millisecond)
	ctxCancelFn()
	wg.Wait()

	//todo print stats.....
	jsonBytes, err := json.Marshal(StatsHistory)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", string(jsonBytes))

}
func calculateSelectBatches() {
	config.SelectsBatches = config.SelectsToRun / config.RampUpSelects
	if config.SelectsToRun%config.RampUpSelects > 0 {
		config.SelectsBatches++
	}
}

func collectStatsLooper(ctx context.Context) {
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			//todo collect stats here
		case <-ctx.Done():
			wg.Done()
			return
		} //end select
	} //end for loop
}

func printProgress(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Print("\n\n")
			wg.Done()
			return
		case <-time.After(100 * time.Millisecond):
			fmt.Printf("\rSelects Fired %d Running %d Completed %d (%d%%)  %ds   ",
				selectsFired,
				db.Stats().InUse,
				selectsCompleted,
				selectsCompleted*100/config.SelectsToRun,
				time.Now().Unix()-StatsHistory.StartTimeUnix)
		} //end select
	} //end for loop
}

func pushConfigValuesToDbObject() {
	setMaxOpenConns(config.MaxOpenConns)
	setMaxIdleConns(config.MaxIdleConns)
	setConnMaxLifetime(config.ConnMaxLifetime)
	setConnMaxIdleTime(config.ConnMaxIdleTime)

	setMysqlMaxOpenConns(config.MysqlMaxOpenConns)
	selectString = fmt.Sprintf("select sleep(%d); -- %s", config.SelectSleepSec, config.MysqlCommandFilterKeyword)
}

func selectsCannon() {
	var selectsToRunThisLoop int
	for selectsFired < config.SelectsToRun {
		if (selectsFired + config.RampUpSelects) > config.SelectsToRun {
			selectsToRunThisLoop = config.SelectsToRun - selectsFired
		} else {
			selectsToRunThisLoop = config.SelectsToRun
		}
		for i := 0; i < selectsToRunThisLoop; i++ {
			selectsFired++
			wgSelects.Add(1)
			go selectRun(selectsFired)
		}
		selectsBatchesFired++
		time.Sleep(time.Duration(config.RampUpIntervalMs) * time.Millisecond)
	}
}

func selectRun(sequence int) {
	defer wgSelects.Done()
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
	config.MaxOpenConns = v
	db.SetMaxOpenConns(v)
}

func getMaxOpenConns() int {
	return config.MaxOpenConns
}

func setMaxIdleConns(v int) {
	config.MaxIdleConns = v
	db.SetMaxIdleConns(v)
}
func getMaxIdleConns() int {
	return config.MaxIdleConns
}

func setConnMaxLifetime(m int) {
	config.ConnMaxLifetime = m
	db.SetConnMaxLifetime(time.Minute * time.Duration(m))
}
func getConnMaxLifetime() int {
	return config.ConnMaxLifetime
}

func setConnMaxIdleTime(m int) {
	config.ConnMaxIdleTime = m
	db.SetConnMaxIdleTime(time.Minute * time.Duration(m))
}
func getConnMaxIdleTime() int {
	return config.ConnMaxIdleTime
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
