package main

import (
	"errors"
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/source"
	"log"
	"strconv"
)

// mysqlDatatype - mysql datatype to graphql scalar
var mysqlDatatype = map[string]graphql.Output{
	"tinyint": graphql.Int,
	"bigint":  graphql.Int,
	"int":     graphql.Int,
	"varchar": graphql.String,
	"char":    graphql.String,
	"text":    graphql.String,
}

var DefaultArgs = map[string]*graphql.ArgumentConfig{
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
}

var supportedComparisonOps = []string{
	"_gt", "_lt", "_gte", "_lte", "_in", "_eq",
}

func GenerateSchema(tableName string, tableSchema TableSchema, filters []string) (*graphql.Schema, error) {
	// Object Type
	objectType := graphql.ObjectConfig{
		Name:   tableName,
		Fields: graphql.Fields{},
	}

	// SelectionList
	//
	// Iterate over MySQL field type and generate GraphQL fields
	fields := graphql.Fields{}
	for fieldName, fieldType := range tableSchema {
		fields[fieldName] = &graphql.Field{
			Type: mysqlDatatype[fieldType],
		}
	}
	objectType.Fields = fields

	// Arguments
	//
	args := graphql.FieldConfigArgument{}
	// Add default arguments
	for field, arg := range DefaultArgs {
		args[field] = arg
	}

	filterFields := graphql.Fields{}
	// Iterate over allowed filters and generate GraphQL filters
	for _, filterField := range filters {
		if fieldType, ok := tableSchema[filterField]; ok {
			field := graphql.Field{}
			fieldArgs := graphql.FieldConfigArgument{}
			for _, op := range supportedComparisonOps {
				fieldArgs[op] = &graphql.ArgumentConfig{
					Type: mysqlDatatype[fieldType],
				}
			}
			field.Type = graphql.String
			field.Args = fieldArgs
			filterFields[filterField] = &field
		}
	}

	// If no field is specified then create filter on all fields
	if len(filters) == 0 {
		for filterField, fieldType := range tableSchema {
			field := graphql.Field{}
			fieldArgs := graphql.FieldConfigArgument{}
			for _, op := range supportedComparisonOps {
				fieldArgs[op] = &graphql.ArgumentConfig{
					Type: mysqlDatatype[fieldType],
				}
			}
			field.Type = graphql.String
			field.Args = fieldArgs
			filterFields[filterField] = &field
		}
	}

	// Where field arguments
	args["where"] = &graphql.ArgumentConfig{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "where",
			Description: "where condition",
			Fields:      filterFields,
		}),
	}

	// Iterate over MySQL fields and generate GraphQL argument field for order_by
	orderFields := graphql.Fields{}
	for fieldName, fieldType := range tableSchema {
		orderFields[fieldName] = &graphql.Field{
			Type: mysqlDatatype[fieldType],
		}
	}

	args["order_by"] = &graphql.ArgumentConfig{
		Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
			Name:   "order_by",
			Fields: orderFields,
		})),
	}
	// Create query object
	var queryType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				tableName: &graphql.Field{
					Type:    graphql.NewList(graphql.NewObject(objectType)),
					Args:    args,
					Resolve: DefaultResolverFn,
				},
			},
		})

	var schema, err = graphql.NewSchema(
		graphql.SchemaConfig{
			Query: queryType,
		},
	)

	if err != nil {
		return nil, err
	}

	return &schema, nil
}

func DefaultResolverFn(params graphql.ResolveParams) (interface{}, error) {
	// Use (GraphQL AST) to create (RQL) -> (generated SQL).
	arguments := GetArguments(params)
	projection := GetProjection(params)

	selectDef := NewSelectDefinition("payments")

	var filter map[string]interface{}
	if value, ok := arguments["where"].(map[string]interface{}); ok {
		filter = value
	}

	limit := 100
	if value, ok := arguments["limit"].(int64); ok {
		limit = int(value)
	}

	offset := 0
	if value, ok := arguments["offset"].(int64); ok {
		offset = int(value)
	}

	var orderBy []map[string]interface{}
	if value, ok := arguments["order_by"].([]map[string]interface{}); ok {
		orderBy = value
	}

	generatedSQLQuery, err := selectDef.
		WithFilters(filter).
		WithPagination(offset, limit).
		WithProjections(projection).
		WithSortCriteria(orderBy).Build()

	result, err := mysql.FetchScan(generatedSQLQuery.(string), limit)
	if err != nil {
		return nil, err
	}

	return result.Rows, nil
}

func Query(query string) (interface{}, error) {
	syntaxTree, err := parser.Parse(parser.ParseParams{Source: source.NewSource(&source.Source{
		Body: []byte(query),
		Name: "request",
	})})

	if err != nil {
		return nil, err
	}

	entity := getEntityName(syntaxTree)
	if entity == "" {
		return nil, errors.New("no entity in query")
	}

	tableSchema, err := mysql.GetTableSchema(entity)
	if err != nil {
		log.Fatalf("failed to get table schema, error: %v", err)
		return nil, err
	}

	graphqlSchema, err := GenerateSchema(Entities.GetTableName(entity), tableSchema, Entities.GetAllowedFilters(entity))
	if err != nil {
		log.Fatalf("failed to create new schema, error: %v", err)
		return nil, err
	}

	params := graphql.Params{Schema: *graphqlSchema, RequestString: query}
	result := graphql.Do(params)

	if result.HasErrors() {
		fmt.Printf("%+v\n", result.Errors)
	}

	return result.Data, nil
}

func GetArguments(params graphql.ResolveParams) map[string]interface{} {
	selection := params.Info.Operation.GetSelectionSet()
	argument := map[string]interface{}{}

	if firstField, ok := selection.Selections[0].(*ast.Field); ok {
		for _, arg := range firstField.Arguments {
			switch arg.Value.GetKind() {
			case "ObjectValue":
				if value, ok := arg.Value.(*ast.ObjectValue); ok {
					argument[arg.Name.Value] = objectValueArg(value)
				}
			case "ListValue":
				if value, ok := arg.Value.(*ast.ListValue); ok {
					argument[arg.Name.Value] = listValueArg(value)
				}
			case "IntValue":
				fallthrough
			case "StringValue":
				fallthrough
			case "BooleanValue":
				if value, ok := arg.Value.(ast.Value); ok {
					argument[arg.Name.Value] = scalarArg(value)
				}
			}
		}
	}

	return argument
}

func listValueArg(listValue *ast.ListValue) []map[string]interface{} {
	var listArgument []map[string]interface{}

	for _, value := range listValue.Values {
		switch value.GetKind() {
		case "ObjectValue":
			if value, ok := value.(*ast.ObjectValue); ok {
				listArgument = append(listArgument, objectValueArg(value))
			}
		}
	}

	return listArgument
}

func objectValueArg(object *ast.ObjectValue) map[string]interface{} {
	argument := map[string]interface{}{}

	for _, field := range object.Fields {
		switch field.Value.GetKind() {
		case "ObjectValue":
			if value, ok := field.Value.(*ast.ObjectValue); ok {
				argument[field.Name.Value] = objectValueArg(value)
			}
		case "IntValue":
			fallthrough
		case "StringValue":
			fallthrough
		case "BooleanValue":
			if value, ok := field.Value.(ast.Value); ok {
				argument[field.Name.Value] = scalarArg(value)
			}
		}
	}

	return argument
}

func scalarArg(scalar ast.Value) interface{} {
	value := scalar.GetValue()

	switch scalar.GetKind() {
	case "IntValue":
		v, _ := strconv.ParseInt(value.(string), 10, 64)
		return v
	case "StringValue":
		return value.(string)
	case "BooleanValue":
		v, _ := strconv.ParseBool(value.(string))
		return v
	}

	return scalar.GetValue()
}

func getEntityName(document *ast.Document) string {
	if len(document.Definitions) > 0 {
		// query operation
		if firstDefinition, ok := document.Definitions[0].(*ast.OperationDefinition); ok {
			selectionSet := firstDefinition.GetSelectionSet()

			// selection set
			if len(selectionSet.Selections) > 0 {
				if firstSelection, ok := selectionSet.Selections[0].(*ast.Field); ok {
					return firstSelection.Name.Value
				}
			}
		}
	}

	return ""
}

func GetProjection(params graphql.ResolveParams) []string {
	selectionSet := params.Info.Operation.GetSelectionSet()
	var projection []string

	for _, value := range selectionSet.Selections {
		if selection, ok := value.(*ast.Field); ok {
			for _, field := range selection.SelectionSet.Selections {
				if fieldDef, ok := field.(*ast.Field); ok {
					projection = append(projection, fieldDef.Name.Value)
				}
			}
		}
	}

	return projection
}
