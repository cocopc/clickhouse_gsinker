package output

import (
	"database/sql"
	"github.com/k0kubun/pp"
	"log"
	"testing"
	"time"
)

func TestLoadBlance(t *testing.T)  {

	connect, err := sql.Open("clickhouse", "tcp://bj-dcs-009:9011?username=default&password=coohua&compress=true&debug=true&alt_hosts=bj-dcs-010:9011")
	checkErr(err)
	// open concurrent transactions
	go exec(connect)
	go exec(connect)
	time.Sleep(10*time.Second)
	exec(connect)

}

func exec(connect *sql.DB){
	for i:=0;i<=3;i++ {
		tx, err:= connect.Begin()
		log.Println (tx,err)
		if err!=nil{
			pp.Println(err)
		}

		stmt, err:= tx.Prepare("insert into table default.newsearn_test(logday,userid,price) values ('?','?','?')")
		if err!=nil{
			log.Println (stmt,err)
		}

		defer stmt.Close()
		args:=make([]interface{},3)
		args[0]="2018-09-02"
		args[1]="1"
		args[2]="2"
		stmt.Exec(args...)

		tx.Commit()
		//
		//pp.Println("================")

	}
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}