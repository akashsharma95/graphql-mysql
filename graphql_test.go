package main

import (
	"encoding/json"
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/printer"
	"github.com/graphql-go/graphql/language/source"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestGenerateSchema(t *testing.T) {
	mysql, err := NewMySql(defaultConf)
	assert.Nil(t, err)

	schema, err := mysql.GetTableSchema("test")
	assert.Nil(t, err)

	graphqlSchema, err := GenerateSchema("test", schema, []string{"id"})
	assert.Nil(t, err)

	jsn, _ := json.MarshalIndent(graphqlSchema, "", " ")
	t.Log(string(jsn))
}

func TestGenerateSQLFromGraphQL(t *testing.T) {
	// Schema
	queryType := graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"product": &graphql.Field{
					Type: graphql.NewObject(
						graphql.ObjectConfig{
							Name: "Product",
							Fields: graphql.Fields{
								"id": &graphql.Field{
									Type: graphql.Int,
								},
								"name": &graphql.Field{
									Type: graphql.String,
								},
								"info": &graphql.Field{
									Type: graphql.String,
								},
								"price": &graphql.Field{
									Type: graphql.Float,
								},
							},
						},
					),
					Args: graphql.FieldConfigArgument{
						"offset": {
							Type:         graphql.Int,
							DefaultValue: 0,
							Description:  "Skip rows by offset",
						},
						"limit": {
							Type:         graphql.Int,
							DefaultValue: 100,
							Description:  "Limit no of rows returned by some value",
						},
						"order_by": &graphql.ArgumentConfig{
							Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
								Name: "order_by",
								Fields: graphql.Fields{
									"id": &graphql.Field{
										Type: graphql.String,
									},
								},
							})),
						},
						"where": &graphql.ArgumentConfig{
							Type: graphql.NewObject(graphql.ObjectConfig{
								Name: "where",
								Fields: graphql.Fields{
									"id": &graphql.Field{
										Type: graphql.String,
										Args: graphql.FieldConfigArgument{
											"_gt": &graphql.ArgumentConfig{
												Type: graphql.Int,
											},
											"_lt": &graphql.ArgumentConfig{
												Type: graphql.Int,
											},
											"_gte": &graphql.ArgumentConfig{
												Type: graphql.Int,
											},
											"_lte": &graphql.ArgumentConfig{
												Type: graphql.Int,
											},
										},
									},
								},
							}),
						},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						query, err := GenerateSQLFromGraphQL(p)
						if err != nil {
							panic(err)
						}
						fmt.Printf("Generated Query %s \n", query)
						return nil, nil
					},
				},
			},
		},
	)

	schemaConfig := graphql.SchemaConfig{Query: queryType}
	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		log.Fatalf("failed to create new schema, error: %v", err)
	}

	// Query
	query := `
		{
			product(where: {id: {_gt: 1}}, offset: 10, limit: 100, order_by: [{id: "desc"}, {created_at:"asc"}]) {
				id,
				name,
				info,
			}
		}
	`
	params := graphql.Params{Schema: schema, RequestString: query}
	src := source.NewSource(&source.Source{
		Body: []byte(params.RequestString),
		Name: "GraphQL request",
	})

	ast, err := parser.Parse(parser.ParseParams{Source: src})

	validationResult := graphql.ValidateDocument(&params.Schema, ast, nil)
	fmt.Printf("%+v\n", validationResult)

	res := graphql.Do(params)
	fmt.Printf("%+v\n", res.Data)

	fmt.Printf("%+v\n", printer.Print(ast))
	jsn, _ := json.MarshalIndent(ast, "", "  ")
	fmt.Printf("%+v\n", string(jsn))
}

func GenerateSQLFromGraphQL(params graphql.ResolveParams) (string, error) {
	arguments := GetArguments(params)
	projection := GetProjection(params)

	selectDef := NewSelectDefinition("payment")

	filter := arguments["where"].(map[string]interface{})
	limit := arguments["limit"].(int64)
	offset := arguments["offset"].(int64)
	orderBy := arguments["order_by"].([]map[string]interface{})

	generatedSQLQuery, err := selectDef.
		WithFilters(filter).
		WithPagination(int(limit), int(offset)).
		WithProjections(projection).
		WithSortCriteria(orderBy).Build()

	return generatedSQLQuery.(string), err
}
