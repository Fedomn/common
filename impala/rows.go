package impala

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/fedomn/common/impala/gen-go/beeswax"
	"github.com/fedomn/common/impala/gen-go/impalaservice"
)

type State struct {
	state *beeswax.QueryState
	Error error
}

func (s *State) string() string {
	return fmt.Sprintf("QueryState: %s, Error: %s", s.state, s.Error)
}

func (s *State) IsSuccess() bool {
	if s.Error != nil {
		return false
	}
	return *s.state != beeswax.QueryState_EXCEPTION
}

func (s *State) IsComplete() bool {
	if s.Error != nil {
		return false
	}
	return *s.state == beeswax.QueryState_FINISHED
}

type Rows struct {
	client *impalaservice.ImpalaServiceClient
	handle *beeswax.QueryHandle

	// 一次fetch获得batch_size行
	rowsSet *beeswax.Results
	// 记录遍历rowsSet.Data的偏移
	offset  int
	hasMore bool
	ready   bool

	columns  []string
	metadata *beeswax.ResultsMetadata
	nextRow  []string
}

func (r *Rows) GetState() (*State, error) {
	state, err := r.client.GetState(context.Background(), r.handle)
	if err != nil || state == beeswax.QueryState_EXCEPTION {
		return nil, err
	}

	return &State{&state, nil}, nil
}

const WaitPollTick = time.Millisecond * 100

const WaitTimeout = time.Second * 10

func (r *Rows) WaitSuccess() (*State, error) {
	c := make(chan State, 1)

	go func() {
		for {
			state, err := r.GetState()
			if err != nil {
				c <- State{nil, err}
				return
			}

			if state.IsComplete() {
				if state.IsSuccess() {
					r.ready = true
					c <- *state
					return
				}
				c <- State{nil, fmt.Errorf("query failed: %s", state.state.String())}
				return
			}
			time.Sleep(WaitPollTick)
		}
	}()

	select {
	case s := <-c:
		return &s, nil
	case <-time.After(WaitTimeout):
		return nil, fmt.Errorf("wait state timeout")
	}
}

func (r *Rows) CheckSuccess() error {
	if !r.ready {
		state, err := r.WaitSuccess()
		if err != nil {
			return err
		}
		if !state.IsSuccess() {
			return fmt.Errorf("state: %s, not success", state)
		}
	}
	return nil
}

const MaxFetchSize = 100000

func (r *Rows) Next() bool {
	if err := r.CheckSuccess(); err != nil {
		log.Printf("check err: %v\n", err)
		return false
	}

	if r.rowsSet == nil || r.offset >= len(r.rowsSet.Data) {
		if !r.hasMore {
			return false
		}

		resp, err := r.client.Fetch(context.Background(), r.handle, false, MaxFetchSize)
		if err != nil {
			log.Printf("fetch err: %v\n", err)
			return false
		}

		if r.metadata == nil {
			r.metadata, err = r.client.GetResultsMetadata(context.Background(), r.handle)
			if err != nil {
				log.Printf("get metadata err:%v\n", err)
			}
		}

		if len(r.columns) == 0 {
			r.columns = resp.Columns
		}

		r.rowsSet = resp
		r.hasMore = resp.HasMore
		r.offset = 0

		if len(resp.Data) == 0 {
			return false
		}
	}

	r.nextRow = strings.Split(r.rowsSet.Data[r.offset], "\t")
	r.offset++

	return true
}

func (r *Rows) Scan(dest ...interface{}) error {
	if r.nextRow == nil || len(r.nextRow) == 0 {
		return errors.New("nextRows is empty")
	}

	if len(dest) != len(r.nextRow) {
		return fmt.Errorf("can't scan into %d args with input of length %d", len(dest), len(r.nextRow))
	}

	for i, v := range r.nextRow {
		switch dt := dest[i].(type) {
		case *string:
			*dt = v
		case *int:
			i, _ := strconv.ParseInt(v, 10, 0)
			*dt = int(i)
		case *int64:
			i, _ := strconv.ParseInt(v, 10, 0)
			*dt = int64(i)
		case *int32:
			i, _ := strconv.ParseInt(v, 10, 0)
			*dt = int32(i)
		case *int16:
			i, _ := strconv.ParseInt(v, 10, 0)
			*dt = int16(i)
		case *float64:
			*dt, _ = strconv.ParseFloat(v, 64)
		default:
			return fmt.Errorf("cat't scan val of type %T with val %v", dt, v)
		}
	}

	return nil
}

func (r *Rows) convertRawValue(raw string, hiveType string) (interface{}, error) {
	switch hiveType {
	case "string":
		return raw, nil
	case "int", "tinyint", "smallint":
		i, err := strconv.ParseInt(raw, 10, 0)
		return int32(i), err
	case "bigint":
		i, err := strconv.ParseInt(raw, 10, 0)
		return int64(i), err
	case "float", "double", "decimal":
		i, err := strconv.ParseFloat(raw, 64)
		return i, err
	case "timestamp":
		i, err := time.Parse("2006-01-02 15:04:05", raw)
		return i, err
	case "boolean":
		return raw == "true", nil
	default:
		return nil, errors.New(fmt.Sprintf("invalid hive type %v", hiveType))
	}
}

func (r *Rows) FetchAll() []map[string]interface{} {
	data := make([]map[string]interface{}, 0)
	for r.Next() {
		row := make(map[string]interface{}, 0)
		for i, v := range r.nextRow {
			conv, err := r.convertRawValue(v, r.metadata.Schema.FieldSchemas[i].Type)
			if err != nil {
				log.Printf("convert raw val err: %v", err)
			}
			row[r.metadata.Schema.FieldSchemas[i].Name] = conv
		}
		data = append(data, row)
	}
	return data
}

// Cancel execution of query. Returns RUNTIME_ERROR if query_id unknown.
// This terminates all threads running on behalf of this query at
// all nodes that were involved in the execution.
// Throws BeeswaxException if the query handle is invalid (this doesn't
// necessarily indicate an error: the query might have finished).
func (r *Rows) Cancel() error {
	_, err := r.client.Cancel(context.Background(), r.handle)
	return err
}

func newRows(client *impalaservice.ImpalaServiceClient, handle *beeswax.QueryHandle) *Rows {
	return &Rows{
		client:   client,
		handle:   handle,
		rowsSet:  nil,
		offset:   0,
		hasMore:  true,
		ready:    false,
		columns:  nil,
		metadata: nil,
		nextRow:  nil,
	}
}
