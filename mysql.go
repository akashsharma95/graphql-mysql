package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const ConnectionDSNFormat = "%s:%s@%s(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local"

var (
	mysqlOnce sync.Once
	mysql     *MySql
)

type MySql struct {
	Db *sql.DB
}

func NewMySql(c SqlConfig) (*MySql, error) {
	var err error

	mysqlOnce.Do(func() {
		mysql = &MySql{}
		mysql.Db, err = sql.Open("mysql", c.GetConnectionPath())
		if err != nil {
			panic(err)
		}
	})
	return mysql, err
}

func (m MySql) Instance() interface{} {
	return m.Db
}

type SqlConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Name     string
	Protocol string
}

func (c SqlConfig) GetConnectionPath() string {
	return fmt.Sprintf(ConnectionDSNFormat, c.Username, c.Password, c.Protocol, c.Host, c.Port, c.Name)
}

type Result struct {
	Rows  []map[string]interface{}
	Count int
}

func (r Result) GetRows() []map[string]interface{} {
	return r.Rows
}

func (r Result) GetCount() int {
	return r.Count
}

func (r Result) MarshalJSON() ([]byte, error) {
	serialized, err := json.Marshal(r.Rows)
	return serialized, err
}

type TableSchema map[string]string

var dataTypeRegex = regexp.MustCompile(`^\w+`)

func (m *MySql) GetTableSchema(table string) (TableSchema, error) {
	rows, err := m.Db.Query("DESC " + table)
	if err != nil {
		return nil, err
	}

	schema := TableSchema{}
	for rows.Next() {
		var (
			fieldName string
			fieldType string
			ignore    interface{}
		)

		if err := rows.Scan(&fieldName, &fieldType, &ignore, &ignore, &ignore, &ignore); err != nil {
			return nil, err
		}
		t := dataTypeRegex.FindAllString(fieldType, 1)
		schema[fieldName] = t[0]
	}

	return schema, nil
}

func (m *MySql) Fetch(query string, limit int) (*Result, error) {
	sqlConn := m.Instance().(*sql.DB)

	rows, err := sqlConn.Query(query)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	rowCount := 0
	var rowSet []map[string]interface{}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPtr := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPtr[i] = &columns[i]
		}

		if err := rows.Scan(columnPtr...); err != nil {
			return nil, err
		}

		row := map[string]interface{}{}
		for i, colName := range cols {
			val := columnPtr[i].(*interface{})
			row[colName] = *val
		}
		rowSet = append(rowSet, row)
		rowCount += 1
	}

	return &Result{
		Rows:  rowSet,
		Count: rowCount,
	}, err
}

func (m *MySql) FetchScan(query string, limit int) (*Result, error) {
	sqlConn := m.Instance().(*sql.DB)

	rows, err := sqlConn.Query(query)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	rowCount := 0
	var rowSet []map[string]interface{}
	for rows.Next() {
		columnPtr := make([]interface{}, len(cols))
		for idx, _ := range cols {
			columnPtr[idx] = new(ColValueScanner)
		}

		err := rows.Scan(columnPtr...)
		if err != nil {
			return nil, err
		}

		row := map[string]interface{}{}
		for idx, column := range cols {
			var scanner = columnPtr[idx].(*ColValueScanner)
			row[column] = scanner.value
		}
		rowSet = append(rowSet, row)
		rowCount += 1
	}

	return &Result{
		Rows:  rowSet,
		Count: rowCount,
	}, err
}

type ColValueScanner struct {
	value interface{}
}

func (scanner *ColValueScanner) getBytes(src interface{}) []byte {
	if a, ok := src.([]uint8); ok {
		return a
	}
	return nil
}

func (scanner *ColValueScanner) Scan(src interface{}) error {
	switch src.(type) {
	case int64:
		if value, ok := src.(int64); ok {
			scanner.value = value
		}
	case float64:
		if value, ok := src.(float64); ok {
			scanner.value = value
		}
	case bool:
		if value, ok := src.(bool); ok {
			scanner.value = value
		}
	case string:
		value := scanner.getBytes(src)
		scanner.value = string(value)
	case []byte:
		value := string(scanner.getBytes(src))
		scanner.value = value
	case time.Time:
		if value, ok := src.(time.Time); ok {
			scanner.value = value
		}
	case nil:
		scanner.value = nil
	}
	return nil
}
