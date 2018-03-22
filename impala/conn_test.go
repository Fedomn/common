package impala_test

import (
	. "fedomn/common/impala"
	"log"
	"testing"
)

func TestNewConn(t *testing.T) {
	conn, err := NewConn(host, port)
	if err != nil {
		log.Println("NewConn err: ", err)
	} else {
		log.Printf("conn %+v", conn)
	}

	sql := "select channel_id, date_flag, mau from result.result_month_base_data_sum limit 10;"
	rows, err := conn.CreateQuery(sql)
	if err != nil {
		log.Println("CreateQuery err: ", err)
		return
	}

	log.Println("conn is open: ", conn.IsOpen())

	err = conn.Close()
	if err != nil {
		log.Printf("conn close err: %+v", err)
	}

	log.Println("conn is open: ", conn.IsOpen())

	all := rows.FetchAll()
	log.Println(all)
}
