package impala_test

import (
	"log"
	"testing"

	. "github.com/fedomn/common/impala"
)

const host = "127.0.0.1"
const port = 21000

func TestImpalaScan(t *testing.T) {
	conn, err := NewConn(host, port)
	if err != nil {
		log.Println("NewConn err: ", err)
		return
	}
	sql := "select channel_id, date_flag, mau from result.result_month_base_data_sum limit 10;"
	rows, err := conn.CreateQuery(sql)
	if err != nil {
		log.Println("CreateQuery err: ", err)
		return
	}

	for rows.Next() {
		var id int
		var dateFlag string
		var mau int
		err := rows.Scan(&id, &dateFlag, &mau)
		if err != nil {
			log.Println("scan err: ", err)
		}
		log.Println(id, dateFlag, mau)
	}
}

func TestImpalaFetchAll(t *testing.T) {
	conn, err := NewConn(host, port)
	if err != nil {
		log.Println("NewConn err: ", err)
		return
	}
	sql := "select channel_id, date_flag, mau from result.result_month_base_data_sum limit 10;"
	rows, err := conn.CreateQuery(sql)
	if err != nil {
		log.Println("CreateQuery err: ", err)
		return
	}

	all := rows.FetchAll()
	log.Println(all)
}

func TestQueryCancel(t *testing.T) {
	conn, err := NewConn(host, port)
	if err != nil {
		log.Println("NewConn err: ", err)
		return
	}
	sql := "select channel_id, date_flag, mau from result.result_month_base_data_sum limit 10;"
	rows, err := conn.CreateQuery(sql)
	if err != nil {
		log.Println("CreateQuery err: ", err)
		return
	}

	err = rows.Cancel()
	log.Println("rows cancel err: ", err)
	all := rows.FetchAll()
	log.Println(all)
}
