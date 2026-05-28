//go:build docker

package dataset

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Load bulk-inserts the dataset into Postgres using COPY FROM in
// dependency order. Returns the first error encountered. The caller is
// expected to have already executed the schema DDL.
//
// COPY is used (instead of INSERT) because it bypasses both per-row
// parsing overhead and per-row WAL records: pgoutput emits one Insert
// message per row either way, but the load completes ~100x faster than
// row-by-row INSERTs for the tiny rowcounts used here.
func Load(ctx context.Context, conn *pgx.Conn, ds Dataset) error {
	loaders := []struct {
		table string
		fn    func(context.Context, *pgx.Conn, Dataset) error
	}{
		{"region", loadRegion},
		{"nation", loadNation},
		{"supplier", loadSupplier},
		{"customer", loadCustomer},
		{"part", loadPart},
		{"partsupp", loadPartSupp},
		{"orders", loadOrders},
		{"lineitem", loadLineItem},
	}
	for _, l := range loaders {
		if err := l.fn(ctx, conn, ds); err != nil {
			return fmt.Errorf("load %s: %w", l.table, err)
		}
	}
	return nil
}

func loadRegion(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.Region))
	for i, r := range ds.Region {
		rows[i] = []any{r.RegionKey, r.Name, r.Comment}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"region"}, Columns("region"), pgx.CopyFromRows(rows))
	return err
}

func loadNation(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.Nation))
	for i, r := range ds.Nation {
		rows[i] = []any{r.NationKey, r.Name, r.RegionKey, r.Comment}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"nation"}, Columns("nation"), pgx.CopyFromRows(rows))
	return err
}

func loadSupplier(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.Supplier))
	for i, r := range ds.Supplier {
		rows[i] = []any{r.SuppKey, r.Name, r.Address, r.NationKey, r.Phone, r.AcctBal, r.Comment}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"supplier"}, Columns("supplier"), pgx.CopyFromRows(rows))
	return err
}

func loadCustomer(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.Customer))
	for i, r := range ds.Customer {
		rows[i] = []any{r.CustKey, r.Name, r.Address, r.NationKey, r.Phone, r.AcctBal, r.MktSegmnt, r.Comment}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"customer"}, Columns("customer"), pgx.CopyFromRows(rows))
	return err
}

func loadPart(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.Part))
	for i, r := range ds.Part {
		rows[i] = []any{r.PartKey, r.Name, r.Mfgr, r.Brand, r.Type, r.Size, r.Container, r.RetailPrice, r.Comment}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"part"}, Columns("part"), pgx.CopyFromRows(rows))
	return err
}

func loadPartSupp(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.PartSupp))
	for i, r := range ds.PartSupp {
		rows[i] = []any{r.PartKey, r.SuppKey, r.AvailQty, r.SupplyCost, r.Comment}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"partsupp"}, Columns("partsupp"), pgx.CopyFromRows(rows))
	return err
}

func loadOrders(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.Orders))
	for i, r := range ds.Orders {
		rows[i] = []any{r.OrderKey, r.CustKey, r.OrderStatus, r.TotalPrice, r.OrderDate, r.OrderPriority, r.Clerk, r.ShipPriority, r.Comment}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"orders"}, Columns("orders"), pgx.CopyFromRows(rows))
	return err
}

func loadLineItem(ctx context.Context, c *pgx.Conn, ds Dataset) error {
	rows := make([][]any, len(ds.LineItem))
	for i, r := range ds.LineItem {
		rows[i] = []any{
			r.OrderKey, r.PartKey, r.SuppKey, r.LineNumber,
			r.Quantity, r.ExtendedPrice, r.Discount, r.Tax,
			r.ReturnFlag, r.LineStatus,
			r.ShipDate, r.CommitDate, r.ReceiptDate,
			r.ShipInstruct, r.ShipMode, r.Comment,
		}
	}
	_, err := c.CopyFrom(ctx, pgx.Identifier{"lineitem"}, Columns("lineitem"), pgx.CopyFromRows(rows))
	return err
}
