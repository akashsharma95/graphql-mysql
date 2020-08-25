package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var defaultConf = SqlConfig{
	Host:     "localhost",
	Port:     23306,
	Username: "root",
	Password: "123",
	Name:     "api_live",
	Protocol: "tcp",
}

func TestMySql_GetTableSchema(t *testing.T) {
	mysql, err := NewMySql(defaultConf)
	assert.Nil(t, err)

	schema, err := mysql.GetTableSchema("payments")
	assert.Nil(t, err)

	fmt.Printf("Schema %+v\n", schema)
}
