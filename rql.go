package main

import (
	"fmt"
	"strings"
)

type Querier interface {
	WithFilters(map[string]interface{}) Querier
	WithProjections([]string) Querier
	WithSortCriteria([]map[string]interface{}) Querier
	WithPagination(offset, limit int) Querier
	Build() (interface{}, error)
}

const (
	NotEqual         = "_ne"
	GreaterThan      = "_gt"
	LessThan         = "_lt"
	GreaterThanEqual = "_gte"
	LessThanEqual    = "_lte"
	In               = "_in"
	Equal            = "_eq"
)

var sqlOperator = map[string]string{
	NotEqual:         "!=",
	LessThanEqual:    "<=",
	LessThan:         "<",
	GreaterThan:      ">",
	GreaterThanEqual: ">=",
	In:               "IN",
	Equal:            "=",
}

type SelectDefinition struct {
	database         string
	table            string
	fieldsFragment   string
	whereFragment    string
	sortOrder        string
	limitFragment    string
	offsetFragment   string
	generatedSqlStmt string
}

func NewSelectDefinition(table string) Querier {
	return &SelectDefinition{
		table: table,
	}
}

// WithFilters - Translate all filter criteria specified as filter to a sql where clause
func (s *SelectDefinition) WithFilters(filters map[string]interface{}) Querier {
	for field, condition := range filters {
		// if filter has condition with some operator eg { "amount": {"$gte": 1000000 }}
		// in that case condition will be a map of operator and condition value
		// by default we consider condition without operator as equivalence
		conditionMap, isMap := condition.(map[string]interface{})
		if isMap {
			for operator, value := range conditionMap {
				// Check if it's really an operator or some junk data
				// TODO: This should be checked by query validator
				if strings.HasPrefix(operator, "_") {

					if strValue, isString := value.(string); isString {
						value = fmt.Sprintf("'%s'", strValue)
					}
					s.whereFragment += fmt.Sprintf("`%s` %s %v AND ", field, sqlOperator[operator], value)
				}
			}
		} else {
			if strValue, isString := condition.(string); isString {
				condition = fmt.Sprintf("'%s'", strValue)
			}
			s.whereFragment += fmt.Sprintf("`%s` %s %v AND ", field, "=", condition)
		}
	}
	s.whereFragment = strings.TrimSuffix(s.whereFragment, " AND ")

	return s
}

// WithProjections - Translate all projection to sql select columns
func (s *SelectDefinition) WithProjections(projection []string) Querier {
	for _, field := range projection {
		s.fieldsFragment += fmt.Sprintf("`%s`, ", field)
	}
	s.fieldsFragment = strings.TrimSuffix(s.fieldsFragment, ", ")

	return s
}

// WithSortCriteria - Translate all sort criteria to sql sort order
func (s *SelectDefinition) WithSortCriteria(sortOrder []map[string]interface{}) Querier {
	for _, criteria := range sortOrder {
		for field, order := range criteria {
			s.sortOrder += fmt.Sprintf("`%s` %s, ", field, order)
		}
	}
	s.sortOrder = strings.TrimSuffix(s.sortOrder, ", ")

	return s
}

// WithPagination - Translate limit and offset in pagination to sql limit and offset
func (s *SelectDefinition) WithPagination(offset, limit int) Querier {
	s.limitFragment = fmt.Sprintf("%d", limit)
	s.offsetFragment = fmt.Sprintf("%d", offset)

	return s
}

func (s *SelectDefinition) Build() (interface{}, error) {
	if len(s.fieldsFragment) == 0 {
		s.fieldsFragment = "*"
	}

	s.generatedSqlStmt = fmt.Sprintf("SELECT %s FROM `%s`", s.fieldsFragment, s.table)

	if len(s.whereFragment) > 0 {
		s.generatedSqlStmt += fmt.Sprintf(" WHERE %s", s.whereFragment)
	}

	if len(s.sortOrder) > 0 {
		s.generatedSqlStmt += fmt.Sprintf(" ORDER BY %s", s.sortOrder)
	}

	if len(s.limitFragment) > 0 {
		s.generatedSqlStmt += fmt.Sprintf(" LIMIT %s", s.limitFragment)
	}

	if len(s.offsetFragment) > 0 {
		s.generatedSqlStmt += fmt.Sprintf(" OFFSET %s", s.offsetFragment)
	}

	s.generatedSqlStmt = strings.TrimRight(s.generatedSqlStmt, " ")
	s.generatedSqlStmt += ";"

	return s.generatedSqlStmt, nil
}

// TODO: Implement this (IN query)
func (s *SelectDefinition) applyOperator(op string, field string, args ...interface{}) string {
	switch op {
	case NotEqual:
	case LessThanEqual:
	case LessThan:
	case GreaterThan:
	case GreaterThanEqual:
		return fmt.Sprintf("`%s` %s %v AND ", field, sqlOperator[op], args[0])
	case In:
		values := make([]string, len(args))
		inValues := strings.Join(values, ", ")
		return fmt.Sprintf("`%s` %s %v AND ", field, sqlOperator[op], inValues)
	}

	return ""
}
