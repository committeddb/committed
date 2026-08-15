//go:build docker || integration

package iceberg_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/iceberg"
)

const (
	minioUser = "minioadmin"
	minioPass = "minioadmin"
)

// icebergStack is the dockerized landing zone: minio (S3) + an Iceberg REST
// catalog wired to it over a shared network, with host-mapped endpoints for
// the sink under test.
type icebergStack struct {
	catalogURI  string // host-mapped REST catalog
	s3Endpoint  string // host-mapped minio
	networkName string
	minioAlias  string // in-network minio endpoint (for the duckdb oracle)
}

func startIcebergStack(t *testing.T) *icebergStack {
	t.Helper()
	ctx := context.Background()

	net, err := network.New(ctx)
	require.NoError(t, err)
	testcontainers.CleanupNetwork(t, net)

	minioC, err := tcminio.Run(ctx, "minio/minio:latest",
		tcminio.WithUsername(minioUser), tcminio.WithPassword(minioPass),
		network.WithNetwork([]string{"minio"}, net))
	testcontainers.CleanupContainer(t, minioC)
	require.NoError(t, err)
	s3Host, err := minioC.ConnectionString(ctx)
	require.NoError(t, err)

	// Create the warehouse bucket.
	_, _, err = minioC.Exec(ctx, []string{"mc", "alias", "set", "local", "http://localhost:9000", minioUser, minioPass})
	require.NoError(t, err)
	_, _, err = minioC.Exec(ctx, []string{"mc", "mb", "local/warehouse"})
	require.NoError(t, err)

	restReq := testcontainers.ContainerRequest{
		Image:        "tabulario/iceberg-rest:latest",
		ExposedPorts: []string{"8181/tcp"},
		Env: map[string]string{
			"CATALOG_WAREHOUSE":              "s3://warehouse/",
			"CATALOG_IO__IMPL":               "org.apache.iceberg.aws.s3.S3FileIO",
			"CATALOG_S3_ENDPOINT":            "http://minio:9000",
			"CATALOG_S3_PATH__STYLE__ACCESS": "true",
			"AWS_ACCESS_KEY_ID":              minioUser,
			"AWS_SECRET_ACCESS_KEY":          minioPass,
			"AWS_REGION":                     "us-east-1",
		},
		Networks:   []string{net.Name},
		WaitingFor: wait.ForListeningPort("8181/tcp").WithStartupTimeout(90 * time.Second),
	}
	restC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: restReq, Started: true,
	})
	testcontainers.CleanupContainer(t, restC)
	require.NoError(t, err)
	restHost, err := restC.Host(ctx)
	require.NoError(t, err)
	restPort, err := restC.MappedPort(ctx, "8181/tcp")
	require.NoError(t, err)

	// The sink resolves S3 through the AWS credential chain: env vars.
	t.Setenv("AWS_ACCESS_KEY_ID", minioUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioPass)
	t.Setenv("AWS_REGION", "us-east-1")

	return &icebergStack{
		catalogURI:  fmt.Sprintf("http://%s:%s", restHost, restPort.Port()),
		s3Endpoint:  "http://" + s3Host,
		networkName: net.Name,
		minioAlias:  "http://minio:9000",
	}
}

func (st *icebergStack) sink(t *testing.T, tableName string, flushRows int) *iceberg.Syncable {
	t.Helper()
	toml := fmt.Sprintf(`[iceberg]
topic = "photos"
catalog = %q
namespace = "committed"
table = %q
flushRows = %d
flushInterval = "1h"
[iceberg.props]
"s3.endpoint" = %q
"s3.region" = "us-east-1"
"s3.force-virtual-addressing" = "false"
`, st.catalogURI, tableName, flushRows, st.s3Endpoint)
	v, err := cluster.ParseConfigBytes("text/toml", []byte(toml))
	require.NoError(t, err)
	s, err := (&iceberg.SyncableParser{}).Parse(v, nil)
	require.NoError(t, err)
	return s.(*iceberg.Syncable)
}

var photosType = &cluster.Type{ID: "photos"}

func upsert(key, payload string, gen uint64) *cluster.Entity {
	e := cluster.NewUpsertEntity(photosType, []byte(key), []byte(payload))
	e.Generation = gen
	return e
}

// syncOne drives one single-entity Actual and reports whether it flushed.
func syncOne(t *testing.T, s *iceberg.Syncable, index uint64, e *cluster.Entity) bool {
	t.Helper()
	should, err := s.Sync(context.Background(), &cluster.Actual{Index: index, Entities: []*cluster.Entity{e}})
	require.NoError(t, err)
	return bool(should)
}

// TestIcebergSinkMergeLifecycle drives the whole copy-on-write merge through
// a real REST catalog + S3: upserts land, same-key updates converge, deletes
// remove, a refresh boundary sweeps stale generations, and the
// snapshot-property marker makes replayed flushes idempotent (crash between
// commit and checkpoint).
func TestIcebergSinkMergeLifecycle(t *testing.T) {
	st := startIcebergStack(t)
	s := st.sink(t, "photos_lifecycle", 3)

	// Flush 1: three upserts at generation 1.
	require.False(t, syncOne(t, s, 10, upsert("k1", `{"v":1}`, 1)))
	require.False(t, syncOne(t, s, 11, upsert("k2", `{"v":2}`, 1)))
	require.True(t, syncOne(t, s, 12, upsert("k3", `{"v":3}`, 1)), "row threshold flushes")
	require.Equal(t, map[string]string{"k1": `{"v":1}`, "k2": `{"v":2}`, "k3": `{"v":3}`}, s.ReadRowsForTest(t))

	// Flush 2: update k2, delete k1, add k4 — CoW merge converges by key.
	require.False(t, syncOne(t, s, 20, upsert("k2", `{"v":22}`, 1)))
	require.False(t, syncOne(t, s, 21, cluster.NewDeleteEntity(photosType, []byte("k1"))))
	require.True(t, syncOne(t, s, 22, upsert("k4", `{"v":4}`, 1)))
	require.Equal(t, map[string]string{"k2": `{"v":22}`, "k3": `{"v":3}`, "k4": `{"v":4}`}, s.ReadRowsForTest(t))

	// Flush 3: a refresh pass at epoch 2 re-emits only k2 and k5; its boundary
	// sweeps everything still at generation < 2 (k3, k4 — deleted at the
	// source in the lost window).
	require.False(t, syncOne(t, s, 30, upsert("k2", `{"v":222}`, 2)))
	require.False(t, syncOne(t, s, 31, upsert("k5", `{"v":5}`, 2)))
	require.True(t, syncOne(t, s, 32, cluster.NewRefreshBoundaryEntity(photosType, 2)), "a boundary forces the flush")
	require.Equal(t, map[string]string{"k2": `{"v":222}`, "k5": `{"v":5}`}, s.ReadRowsForTest(t))

	snapshotsBefore := s.SnapshotCountForTest(t)

	// Crash between commit and checkpoint: a FRESH sink instance (restart)
	// replays the exact same last batch. The snapshot-property marker skips
	// the re-commit entirely.
	s2 := st.sink(t, "photos_lifecycle", 3)
	require.False(t, syncOne(t, s2, 30, upsert("k2", `{"v":222}`, 2)))
	require.False(t, syncOne(t, s2, 31, upsert("k5", `{"v":5}`, 2)))
	require.True(t, syncOne(t, s2, 32, cluster.NewRefreshBoundaryEntity(photosType, 2)),
		"the replayed flush reports success so the checkpoint can advance")
	require.Equal(t, snapshotsBefore, s2.SnapshotCountForTest(t), "an already-committed replay must not commit again")
	require.Equal(t, map[string]string{"k2": `{"v":222}`, "k5": `{"v":5}`}, s2.ReadRowsForTest(t))

	// A replay that OVERLAPS a committed range but extends past it re-merges:
	// idempotent by key, no duplicates, the new row lands.
	require.False(t, syncOne(t, s2, 31, upsert("k5", `{"v":5}`, 2)))
	require.False(t, syncOne(t, s2, 32, upsert("k2", `{"v":222}`, 2)))
	require.True(t, syncOne(t, s2, 40, upsert("k6", `{"v":6}`, 2)))
	require.Equal(t, map[string]string{"k2": `{"v":222}`, "k5": `{"v":5}`, "k6": `{"v":6}`}, s2.ReadRowsForTest(t))
}

// TestIcebergSinkDuckDBOracle proves the landing zone with an INDEPENDENT
// reader: duckdb (in a container on the stack's network) scans the table's
// current metadata straight off minio and must see exactly the merged
// current-state rows — the whole point of the sink is that standard engines
// read its tables with no committed in the path.
func TestIcebergSinkDuckDBOracle(t *testing.T) {
	st := startIcebergStack(t)
	s := st.sink(t, "photos_oracle", 2)

	require.False(t, syncOne(t, s, 10, upsert("a", `{"n":1}`, 1)))
	require.True(t, syncOne(t, s, 11, upsert("b", `{"n":2}`, 1)))
	require.False(t, syncOne(t, s, 12, upsert("a", `{"n":10}`, 1)), "update a")
	require.True(t, syncOne(t, s, 13, cluster.NewDeleteEntity(photosType, []byte("b"))), "delete b")
	require.Equal(t, map[string]string{"a": `{"n":10}`}, s.ReadRowsForTest(t))

	metadataLoc := s.MetadataLocationForTest(t) // s3://warehouse/…/metadata.json

	sql := strings.Join([]string{
		"INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs",
		"SET s3_endpoint='minio:9000'",
		"SET s3_use_ssl=false", "SET s3_url_style='path'",
		fmt.Sprintf("SET s3_access_key_id='%s'", minioUser),
		fmt.Sprintf("SET s3_secret_access_key='%s'", minioPass),
		"SET s3_region='us-east-1'",
		fmt.Sprintf("SELECT key, payload FROM iceberg_scan('%s') ORDER BY key", metadataLoc),
	}, "; ") + ";"

	req := testcontainers.ContainerRequest{
		Image:      "duckdb/duckdb:latest",
		Networks:   []string{st.networkName},
		Cmd:        []string{"duckdb", "-csv", "-c", sql},
		WaitingFor: wait.ForExit().WithExitTimeout(180 * time.Second),
	}
	duck, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	testcontainers.CleanupContainer(t, duck)
	require.NoError(t, err)

	rc, err := duck.Logs(context.Background())
	require.NoError(t, err)
	defer rc.Close()
	buf := new(strings.Builder)
	_, err = fmt.Fprint(buf, readAll(t, rc))
	require.NoError(t, err)
	out := buf.String()

	require.Contains(t, out, `a,"{""n"":10}"`, "duckdb reads the merged current state: %s", out)
	require.NotContains(t, out, "\nb,", "the deleted key is gone from an independent reader's view: %s", out)
}

func readAll(t *testing.T, rc interface{ Read([]byte) (int, error) }) string {
	t.Helper()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := rc.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			return sb.String()
		}
	}
}
