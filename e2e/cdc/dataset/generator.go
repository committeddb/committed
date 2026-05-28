//go:build docker

package dataset

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Dataset is the in-memory baseline state — typed rows for each of the
// 8 TPC-H tables, generated deterministically from a seed. The harness
// loads this into Postgres before the mutation script runs.
type Dataset struct {
	Region   []RegionRow
	Nation   []NationRow
	Supplier []SupplierRow
	Customer []CustomerRow
	Part     []PartRow
	PartSupp []PartSuppRow
	Orders   []OrdersRow
	LineItem []LineItemRow
}

type RegionRow struct {
	RegionKey int
	Name      string
	Comment   string
}

type NationRow struct {
	NationKey int
	Name      string
	RegionKey int
	Comment   string
}

type SupplierRow struct {
	SuppKey   int
	Name      string
	Address   string
	NationKey int
	Phone     string
	AcctBal   string
	Comment   string
}

type CustomerRow struct {
	CustKey   int
	Name      string
	Address   string
	NationKey int
	Phone     string
	AcctBal   string
	MktSegmnt string
	Comment   string
}

type PartRow struct {
	PartKey     int
	Name        string
	Mfgr        string
	Brand       string
	Type        string
	Size        int
	Container   string
	RetailPrice string
	Comment     string
}

type PartSuppRow struct {
	PartKey     int
	SuppKey     int
	AvailQty    int
	SupplyCost  string
	Comment     string
}

type OrdersRow struct {
	OrderKey      int
	CustKey       int
	OrderStatus   string
	TotalPrice    string
	OrderDate     time.Time
	OrderPriority string
	Clerk         string
	ShipPriority  int
	Comment       string
}

type LineItemRow struct {
	OrderKey       int
	PartKey        int
	SuppKey        int
	LineNumber     int
	Quantity       string
	ExtendedPrice  string
	Discount       string
	Tax            string
	ReturnFlag     string
	LineStatus     string
	ShipDate       time.Time
	CommitDate     time.Time
	ReceiptDate    time.Time
	ShipInstruct   string
	ShipMode       string
	Comment        string
}

// Sizes controls per-table rowcounts. Defaults are tiny — picked so the
// full dataset loads in well under a second and produces a small enough
// initial-change-stream that ingestable snapshot completion is fast.
// The harness tests do not depend on TPC-H byte-size targets; SF sweeps
// are a future-phase concern (see plan, out-of-scope).
type Sizes struct {
	Regions       int
	NationsPerRgn int
	Suppliers     int
	Customers     int
	Parts         int
	OrdersCount   int
	LinesPerOrder int
}

// DefaultSizes is the size profile every deterministic scenario test
// uses unless it explicitly overrides. Roughly: 5 regions × 5 nations
// = 25 nations; 20 suppliers; 50 customers; 30 parts; ~30 partsupps;
// 40 orders × ~2 lines = ~80 line items. Total ~250 rows across all
// tables.
var DefaultSizes = Sizes{
	Regions:       5,
	NationsPerRgn: 5,
	Suppliers:     20,
	Customers:     50,
	Parts:         30,
	OrdersCount:   40,
	LinesPerOrder: 2,
}

// Generate builds a deterministic Dataset from a seed. Same seed →
// byte-identical Dataset across runs and across machines (math/rand/v2
// PCG is platform-independent).
func Generate(seed uint64, sizes Sizes) Dataset {
	r := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	ds := Dataset{}

	for i := 0; i < sizes.Regions; i++ {
		ds.Region = append(ds.Region, RegionRow{
			RegionKey: i,
			Name:      fmt.Sprintf("REGION%d", i),
			Comment:   fillComment(r, 60),
		})
	}

	nationKey := 0
	for ri := 0; ri < sizes.Regions; ri++ {
		for ni := 0; ni < sizes.NationsPerRgn; ni++ {
			ds.Nation = append(ds.Nation, NationRow{
				NationKey: nationKey,
				Name:      fmt.Sprintf("NATION%d", nationKey),
				RegionKey: ri,
				Comment:   fillComment(r, 60),
			})
			nationKey++
		}
	}
	nationCount := nationKey

	for i := 0; i < sizes.Suppliers; i++ {
		ds.Supplier = append(ds.Supplier, SupplierRow{
			SuppKey:   i,
			Name:      fmt.Sprintf("Supplier#%05d", i),
			Address:   fillComment(r, 25),
			NationKey: r.IntN(nationCount),
			Phone:     fmt.Sprintf("%015d", r.Int64N(1e15)),
			AcctBal:   money(r),
			Comment:   fillComment(r, 80),
		})
	}

	for i := 0; i < sizes.Customers; i++ {
		ds.Customer = append(ds.Customer, CustomerRow{
			CustKey:   i,
			Name:      fmt.Sprintf("Customer#%05d", i),
			Address:   fillComment(r, 25),
			NationKey: r.IntN(nationCount),
			Phone:     fmt.Sprintf("%015d", r.Int64N(1e15)),
			AcctBal:   money(r),
			MktSegmnt: pickSegment(r),
			Comment:   fillComment(r, 90),
		})
	}

	for i := 0; i < sizes.Parts; i++ {
		ds.Part = append(ds.Part, PartRow{
			PartKey:     i,
			Name:        fmt.Sprintf("part-%05d", i),
			Mfgr:        fmt.Sprintf("Manufacturer#%d", 1+r.IntN(5)),
			Brand:       fmt.Sprintf("Brand#%d%d", 1+r.IntN(5), 1+r.IntN(5)),
			Type:        "STANDARD POLISHED COPPER",
			Size:        1 + r.IntN(50),
			Container:   "SM CASE",
			RetailPrice: money(r),
			Comment:     fillComment(r, 20),
		})
	}

	// PartSupp: every part has one supplier (keeps things tiny). We
	// pick suppliers round-robin to keep the dataset deterministic and
	// well-distributed.
	for i, p := range ds.Part {
		ds.PartSupp = append(ds.PartSupp, PartSuppRow{
			PartKey:    p.PartKey,
			SuppKey:    ds.Supplier[i%sizes.Suppliers].SuppKey,
			AvailQty:   1 + r.IntN(1000),
			SupplyCost: money(r),
			Comment:    fillComment(r, 100),
		})
	}

	baseDate := time.Date(1995, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < sizes.OrdersCount; i++ {
		ds.Orders = append(ds.Orders, OrdersRow{
			OrderKey:      i,
			CustKey:       ds.Customer[r.IntN(len(ds.Customer))].CustKey,
			OrderStatus:   "O",
			TotalPrice:    money(r),
			OrderDate:     baseDate.AddDate(0, 0, r.IntN(365)),
			OrderPriority: "3-MEDIUM",
			Clerk:         fmt.Sprintf("Clerk#%09d", 1+r.IntN(1000)),
			ShipPriority:  0,
			Comment:       fillComment(r, 70),
		})
	}

	for _, o := range ds.Orders {
		for ln := 1; ln <= sizes.LinesPerOrder; ln++ {
			ps := ds.PartSupp[r.IntN(len(ds.PartSupp))]
			shipDate := o.OrderDate.AddDate(0, 0, 1+r.IntN(30))
			ds.LineItem = append(ds.LineItem, LineItemRow{
				OrderKey:      o.OrderKey,
				PartKey:       ps.PartKey,
				SuppKey:       ps.SuppKey,
				LineNumber:    ln,
				Quantity:      money(r),
				ExtendedPrice: money(r),
				Discount:      "0.05",
				Tax:           "0.07",
				ReturnFlag:    "N",
				LineStatus:    "O",
				ShipDate:      shipDate,
				CommitDate:    shipDate.AddDate(0, 0, 1),
				ReceiptDate:   shipDate.AddDate(0, 0, 5),
				ShipInstruct:  "DELIVER IN PERSON",
				ShipMode:      "AIR",
				Comment:       fillComment(r, 35),
			})
		}
	}

	return ds
}

func fillComment(r *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz "
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(b)
}

func money(r *rand.Rand) string {
	return fmt.Sprintf("%d.%02d", r.IntN(10000), r.IntN(100))
}

var segments = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}

func pickSegment(r *rand.Rand) string {
	return segments[r.IntN(len(segments))]
}
