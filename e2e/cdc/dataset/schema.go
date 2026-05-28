//go:build docker

// Package dataset provides the TPC-H schema and a pure-Go tiny-rowcount
// generator used by the CDC pressure-test harness. The schema covers the
// 8 standard TPC-H tables with proper foreign keys and REPLICA IDENTITY
// FULL so deletes/updates carry their pre-image through pgoutput.
package dataset

import "strings"

// SchemaSQL is the PostgreSQL DDL for the 8 TPC-H tables, in dependency
// order (region → nation → supplier → customer → part → partsupp →
// orders → lineitem). Every table is set to REPLICA IDENTITY FULL so
// pgoutput emits full old-row data on UPDATE and DELETE — required by
// any oracle that asserts the before-image of a mutation.
//
// Column types are loosely TPC-H-shaped (DECIMAL where TPC-H specifies
// DECIMAL, VARCHAR for strings, DATE for dates) but not pedantic. The
// harness is testing CDC correctness, not TPC-H query semantics.
const SchemaSQL = `
CREATE TABLE region (
    r_regionkey  INTEGER     NOT NULL PRIMARY KEY,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
);
ALTER TABLE region REPLICA IDENTITY FULL;

CREATE TABLE nation (
    n_nationkey  INTEGER     NOT NULL PRIMARY KEY,
    n_name       VARCHAR(25) NOT NULL,
    n_regionkey  INTEGER     NOT NULL REFERENCES region(r_regionkey),
    n_comment    VARCHAR(152)
);
ALTER TABLE nation REPLICA IDENTITY FULL;

CREATE TABLE supplier (
    s_suppkey    INTEGER       NOT NULL PRIMARY KEY,
    s_name       VARCHAR(25)   NOT NULL,
    s_address    VARCHAR(40)   NOT NULL,
    s_nationkey  INTEGER       NOT NULL REFERENCES nation(n_nationkey),
    s_phone      VARCHAR(15)   NOT NULL,
    s_acctbal    DECIMAL(15,2) NOT NULL,
    s_comment    VARCHAR(101)  NOT NULL
);
ALTER TABLE supplier REPLICA IDENTITY FULL;

CREATE TABLE customer (
    c_custkey     INTEGER       NOT NULL PRIMARY KEY,
    c_name        VARCHAR(25)   NOT NULL,
    c_address     VARCHAR(40)   NOT NULL,
    c_nationkey   INTEGER       NOT NULL REFERENCES nation(n_nationkey),
    c_phone       VARCHAR(15)   NOT NULL,
    c_acctbal     DECIMAL(15,2) NOT NULL,
    c_mktsegment  VARCHAR(10)   NOT NULL,
    c_comment     VARCHAR(117)  NOT NULL
);
ALTER TABLE customer REPLICA IDENTITY FULL;

CREATE TABLE part (
    p_partkey      INTEGER       NOT NULL PRIMARY KEY,
    p_name         VARCHAR(55)   NOT NULL,
    p_mfgr         VARCHAR(25)   NOT NULL,
    p_brand        VARCHAR(10)   NOT NULL,
    p_type         VARCHAR(25)   NOT NULL,
    p_size         INTEGER       NOT NULL,
    p_container    VARCHAR(10)   NOT NULL,
    p_retailprice  DECIMAL(15,2) NOT NULL,
    p_comment      VARCHAR(23)   NOT NULL
);
ALTER TABLE part REPLICA IDENTITY FULL;

CREATE TABLE partsupp (
    ps_partkey     INTEGER       NOT NULL REFERENCES part(p_partkey),
    ps_suppkey     INTEGER       NOT NULL REFERENCES supplier(s_suppkey),
    ps_availqty    INTEGER       NOT NULL,
    ps_supplycost  DECIMAL(15,2) NOT NULL,
    ps_comment     VARCHAR(199)  NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);
ALTER TABLE partsupp REPLICA IDENTITY FULL;

CREATE TABLE orders (
    o_orderkey       INTEGER       NOT NULL PRIMARY KEY,
    o_custkey        INTEGER       NOT NULL REFERENCES customer(c_custkey),
    o_orderstatus    CHAR(1)       NOT NULL,
    o_totalprice     DECIMAL(15,2) NOT NULL,
    o_orderdate      DATE          NOT NULL,
    o_orderpriority  VARCHAR(15)   NOT NULL,
    o_clerk          VARCHAR(15)   NOT NULL,
    o_shippriority   INTEGER       NOT NULL,
    o_comment        VARCHAR(79)   NOT NULL
);
ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE TABLE lineitem (
    l_orderkey       INTEGER       NOT NULL REFERENCES orders(o_orderkey),
    l_partkey        INTEGER       NOT NULL,
    l_suppkey        INTEGER       NOT NULL,
    l_linenumber     INTEGER       NOT NULL,
    l_quantity       DECIMAL(15,2) NOT NULL,
    l_extendedprice  DECIMAL(15,2) NOT NULL,
    l_discount       DECIMAL(15,2) NOT NULL,
    l_tax            DECIMAL(15,2) NOT NULL,
    l_returnflag     CHAR(1)       NOT NULL,
    l_linestatus     CHAR(1)       NOT NULL,
    l_shipdate       DATE          NOT NULL,
    l_commitdate     DATE          NOT NULL,
    l_receiptdate    DATE          NOT NULL,
    l_shipinstruct   VARCHAR(25)   NOT NULL,
    l_shipmode       VARCHAR(10)   NOT NULL,
    l_comment        VARCHAR(44)   NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey)
);
ALTER TABLE lineitem REPLICA IDENTITY FULL;
`

// Tables lists the 8 TPC-H tables in dependency order — parents before
// children. The harness uses this for ordered loads, ordered teardown,
// and for generating per-table ingestable configs.
var Tables = []string{
	"region",
	"nation",
	"supplier",
	"customer",
	"part",
	"partsupp",
	"orders",
	"lineitem",
}

// PrimaryKey returns the single-column primary key for a table, or the
// first column of a composite key. (committed's ingestable config takes
// a single primaryKey column name; for composite PKs like partsupp and
// lineitem we use the leftmost column and the harness expresses the
// composite identity in the Entity's Data JSON.)
func PrimaryKey(table string) string {
	switch table {
	case "region":
		return "r_regionkey"
	case "nation":
		return "n_nationkey"
	case "supplier":
		return "s_suppkey"
	case "customer":
		return "c_custkey"
	case "part":
		return "p_partkey"
	case "partsupp":
		return "ps_partkey"
	case "orders":
		return "o_orderkey"
	case "lineitem":
		return "l_orderkey"
	}
	return ""
}

// Columns returns the ordered list of column names for a table. Order
// matters: the mutation DSL and the dataset loader both rely on stable
// column order to produce identical JSON regardless of map iteration.
func Columns(table string) []string {
	switch table {
	case "region":
		return []string{"r_regionkey", "r_name", "r_comment"}
	case "nation":
		return []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}
	case "supplier":
		return []string{"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"}
	case "customer":
		return []string{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"}
	case "part":
		return []string{"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"}
	case "partsupp":
		return []string{"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"}
	case "orders":
		return []string{"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"}
	case "lineitem":
		return []string{"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"}
	}
	return nil
}

// Statements splits SchemaSQL into individual SQL statements suitable
// for executing one at a time through pgx's Exec. Postgres's
// multi-statement Exec works for most clients but pgx requires each
// statement in its own call when bound parameters are not involved.
func Statements() []string {
	var out []string
	for _, stmt := range strings.Split(SchemaSQL, ";") {
		s := strings.TrimSpace(stmt)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}
