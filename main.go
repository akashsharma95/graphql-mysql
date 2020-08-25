package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var query map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&query)

		result, err := Query(query["query"].(string))
		if err != nil {
			_, _ = w.Write([]byte(fmt.Sprintf("Error : %v", err)))
		}
		_ = json.NewEncoder(w).Encode(result)
	})

	var defaultConf = SqlConfig{
		Host:     "localhost",
		Port:     23306,
		Username: "user",
		Password: "123",
		Name:     "database",
		Protocol: "tcp",
	}

	_, err := NewMySql(defaultConf)
	if err != nil {
		panic(err)
	}

	fmt.Println("Server is running on port 8080")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}
