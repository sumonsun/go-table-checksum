package main

import (
	"flag"
	"go-table-checksum/source"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	configFile = flag.String("config", "../conf/checksum.toml", "go-table-checksum config file")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var err error
	var config *tablechecksum.Config
	config, err = tablechecksum.NewConfigWithFile(*configFile)
	if err != nil {
		log.Fatalf("config load failed.detail=%s", errors.ErrorStack(err))
	}
	if config.Checkinfo.Database == "" || config.Checkinfo.Database == "*" {
		log.Fatalf("暂时还不支持到全库")
		panic("")
	} else {
		var vsource tablechecksum.MysqlsourceConfig
		var vaims tablechecksum.MysqlaimsConfig
		if config.Checkinfo.Table == "" || config.Checkinfo.Table == "*" {
			log.Fatalf("暂时还不支持到全表")
			panic("")
		} else {
			for _, v := range *config.Mysqlsource {
				vsource.Addr = v.Addr
				vsource.Port = v.Port
				vsource.User = v.User
				vsource.Password = v.Password
			}

			for _, v := range *config.Mysqlaims {
				vaims.Addr = v.Addr
				vaims.Port = v.Port
				vaims.User = v.User
				vaims.Password = v.Password
			}

		}
		database := config.Checkinfo.Database
		table := config.Checkinfo.Table
		chunksize := config.Chunksize
		goroutine := config.Goroutine
		tablechecksum.DB_conn(vsource, vaims, database, table, chunksize, goroutine)
	}
	signal := <-sc
	log.Errorf("program terminated! signal:%v", signal)
}
