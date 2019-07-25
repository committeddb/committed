package db

// import (
// 	"bytes"
// 	"context"
// 	"database/sql"
// 	"errors"
// 	"fmt"
// 	"io/ioutil"
// 	"log"
// 	"os"
// 	"reflect"
// 	"time"

// 	"github.com/philborlin/committed/syncable"
// 	"github.com/philborlin/committed/types"

// 	. "github.com/onsi/ginkgo"
// 	. "github.com/onsi/gomega"
// )

// type testReturn struct {
// 	Key string
// 	One string
// }

// var _ = Describe("Cluster", func() {
// 	var (
// 		data []byte
// 		err  error
// 	)

// 	JustBeforeEach(func() {
// 		data, err = ioutil.ReadFile("../syncable/simple.toml")
// 		Expect(err).To(BeNil())
// 	})

// 	It("should correctly add a Syncable", func() {
// 		f := func(c *Cluster) (interface{}, interface{}, error) {
// 			name, sync, err := syncable.Parse("toml", bytes.NewReader(data), c.Data.Databases)
// 			Expect(err).To(BeNil())

// 			err = c.CreateSyncable(name, sync)
// 			Expect(err).To(BeNil())

// 			time.Sleep(2 * time.Second)

// 			sqlSyncable, ok := sync.(*syncable.SQLSyncable)
// 			Expect(ok).To(BeTrue())

// 			e2 := reflect.ValueOf(sqlSyncable).Elem()
// 			db := e2.FieldByName("DB").Interface().(*sql.DB)
// 			defer db.Close()

// 			execInTransaction(db, "CREATE TABLE foo (key string, two string);")

// 			proposal := types.Proposal{Topic: "test1", Proposal: "{\"Key\": \"lock\", \"One\": \"two\"}"}
// 			c.Propose(proposal)

// 			time.Sleep(2 * time.Second)

// 			expected := testReturn{Key: "lock", One: "two"}
// 			var actual testReturn
// 			err = SelectOneRowFromDB(db, "SELECT * FROM foo", &actual.Key, &actual.One)

// 			return expected, actual, err
// 		}

// 		clusterTest(f)
// 	})

// 	It("should correctly add a topic", func() {
// 		f := func(c *Cluster) (interface{}, interface{}, error) {
// 			topicName := "test1"
// 			expected, err := c.CreateTopic(topicName)
// 			actual := c.Data.Topics[topicName]
// 			return expected, actual, err
// 		}

// 		clusterTest(f)
// 	})

// 	It("should correctly add a database", func() {
// 		f := func(c *Cluster) (interface{}, interface{}, error) {
// 			name := "foo"
// 			database := types.NewSQLDB("", "")
// 			err := c.CreateDatabase(name, database)
// 			actual := c.Data.Databases[name]
// 			return database, actual, err
// 		}

// 		clusterTest(f)
// 	})

// 	It("should correctly append to a topic", func() {
// 		f := func(c *Cluster) (interface{}, interface{}, error) {
// 			expected := types.Proposal{Topic: "test1", Proposal: "Hello World"}
// 			c.Propose(expected)
// 			time.Sleep(2 * time.Second)

// 			lastIndex, err := c.storage.LastIndex()
// 			Expect(err).To(BeNil())

// 			entries, err := c.storage.Entries(lastIndex, lastIndex+1, 1)
// 			Expect(err).To(BeNil())

// 			actual, err := decodeProposal(entries[0].Data)

// 			return expected, actual, err
// 		}

// 		clusterTest(f)
// 	})
// })

// func clusterTest(f func(*Cluster) (expected interface{}, actual interface{}, err error)) {
// 	c := NewCluster([]string{"http://127.0.0.1:12379"}, 1, false)

// 	time.AfterFunc(2*time.Second, func() {
// 		log.Printf("Starting test function")
// 		after := func(c *Cluster) {
// 			err := c.Shutdown()
// 			Expect(err).To(BeNil())
// 			file := fmt.Sprintf("raft-%d", 1)
// 			err = os.RemoveAll(file)
// 			Expect(err).To(BeNil())
// 			file = fmt.Sprintf("raft-%d-snap", 1)
// 			err = os.RemoveAll(file)
// 			Expect(err).To(BeNil())
// 		}

// 		expected, actual, err := f(c)
// 		after(c)

// 		Expect(err).To(BeNil())
// 		Expect(expected).To(Equal(actual))
// 	})

// 	go c.Start()
// }

// func execInTransaction(db *sql.DB, sqlString string) {
// 	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: 0, ReadOnly: false})
// 	Expect(err).To(BeNil())

// 	_, err = tx.ExecContext(context.Background(), sqlString)
// 	Expect(err).To(BeNil())

// 	err = tx.Commit()
// 	Expect(err).To(BeNil())
// }

// func SelectOneRowFromDB(db *sql.DB, table string, dest ...interface{}) error {
// 	rows, err := db.Query("SELECT * FROM foo")
// 	if err != nil {
// 		return err
// 	}
// 	defer rows.Close()
// 	var rowCount int
// 	for rows.Next() {
// 		rowCount++
// 		if err := rows.Scan(dest...); err != nil {
// 			return err
// 		}
// 	}
// 	if rowCount != 1 {
// 		return errors.New("Data is not in the database")
// 	}

// 	return rows.Err()
// }
