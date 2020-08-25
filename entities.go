package main

// Entities
const (
	Payments = "payments"
)

const (
	AllowedFilter = "allowed_filter"
	Relations     = "relations"
	TableName     = "table_name"
)

type EntityConfig map[string]interface{}

var Entities = EntityConfig{
	Payments: map[string]interface{}{
		TableName: "payments",
	},
}

func (e EntityConfig) getConfigValue(entity, configKey string) interface{} {
	if config, ok := e[entity]; ok {
		if value, ok := config.(map[string]interface{})[configKey]; ok {
			return value
		}
	}
	return nil
}

func (e EntityConfig) GetAllowedFilters(entity string) []string {
	if config := e.getConfigValue(entity, AllowedFilter); config != nil {
		return config.([]string)
	}

	return []string{}
}

func (e EntityConfig) GetRelations(entity string) []string {
	if config := e.getConfigValue(entity, Relations); config != nil {
		return config.([]string)
	}

	return []string{}
}

func (e EntityConfig) GetTableName(entity string) string {
	if config := e.getConfigValue(entity, TableName); config != nil {
		return config.(string)
	}

	return ""
}
