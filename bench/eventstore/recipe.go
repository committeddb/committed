package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// recipe holds the recipe's configs, as published, and the changes the recipe
// itself says a different engine needs.
type recipe struct {
	eventsDDL                         string
	typ, ingestable, database, orders string
}

var fenceRE = regexp.MustCompile("(?s)```([a-z]*)\n(.*?)```")

func loadRecipe(path string) (*recipe, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: the operator names the recipe
	if err != nil {
		return nil, err
	}
	r := &recipe{}
	for _, m := range fenceRE.FindAllStringSubmatch(string(b), -1) {
		lang, body := m[1], m[2]
		switch {
		case lang == "sql" && strings.Contains(body, "CREATE TABLE events"):
			r.eventsDDL = body
		case lang != "toml":
		case strings.HasPrefix(body, "# POST /v1/type/order-events") && !strings.Contains(body, "onSupersede"):
			r.typ = body
		case strings.HasPrefix(body, "# POST /v1/ingestable/order-events"):
			r.ingestable = body
		case strings.HasPrefix(body, "# POST /v1/database/read"):
			r.database = body
		case strings.HasPrefix(body, "# POST /v1/syncable/orders"):
			r.orders = body
		}
	}
	for name, v := range map[string]string{
		"CREATE TABLE events": r.eventsDDL, "type": r.typ, "ingestable": r.ingestable,
		"database": r.database, "projection": r.orders,
	} {
		if v == "" {
			return nil, fmt.Errorf("%s: no %s block", path, name)
		}
	}
	return r, nil
}

// ingestableFor is the recipe's ingestable for the engine: the dialect and
// connection string change, [sql.options] is Postgres's, and a text payload
// column is listed under jsonColumns (the recipe's step 1 bullets).
func (r *recipe) ingestableFor(o options) (string, error) {
	m, err := decode(r.ingestable)
	if err != nil {
		return "", err
	}
	sql, ok := m["sql"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("the recipe's ingestable has no [sql] table")
	}
	sql["dialect"] = o.engine
	sql["connectionString"] = o.srcURL
	if o.engine != "postgres" {
		delete(sql, "options")
	}
	if o.engine == "sqlserver" && o.sqlserverPoll != "" {
		sql["options"] = map[string]any{"pollInterval": o.sqlserverPoll}
	}
	p := strings.ToUpper(o.payload)
	if p == "TEXT" || strings.HasPrefix(p, "NVARCHAR") || strings.HasPrefix(p, "VARCHAR") {
		sql["jsonColumns"] = []string{"data"}
	}
	return encode(m)
}

func (r *recipe) databaseFor(o options) (string, error) {
	m, err := decode(r.database)
	if err != nil {
		return "", err
	}
	sql, ok := m["sql"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("the recipe's database has no [sql] table")
	}
	sql["connectionString"] = o.readURL
	return encode(m)
}

// projectionNamed is the recipe's projection under another name and table:
// a second read model built from the start of the log (the recipe's step 3).
func (r *recipe) projectionNamed(name string) (string, error) {
	m, err := decode(r.orders)
	if err != nil {
		return "", err
	}
	s, ok1 := m["syncable"].(map[string]any)
	p, ok2 := m["projection"].(map[string]any)
	if !ok1 || !ok2 {
		return "", fmt.Errorf("the recipe's projection has no [syncable] or [projection] table")
	}
	s["name"] = name
	p["table"] = name
	return encode(m)
}

func decode(s string) (map[string]any, error) {
	m := map[string]any{}
	return m, toml.Unmarshal([]byte(s), &m)
}

func encode(m map[string]any) (string, error) {
	b, err := toml.Marshal(m)
	return string(b), err
}
