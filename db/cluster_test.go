package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/types"
	"github.com/philborlin/committed/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func clusterTest(t *testing.T, f func(*Cluster) (expected interface{}, actual interface{}, err error)) {
	c := NewCluster([]string{"http://127.0.0.1:12379"}, 1, false)

	time.AfterFunc(2*time.Second, func() {
		log.Printf("Starting test function")
		after := func(c *Cluster) {
			if err := c.Shutdown(); err != nil {
				fmt.Println("Error shutting down the cluster")
			}
			file := fmt.Sprintf("raft-%d", 1)
			if err := os.RemoveAll(file); err != nil {
				fmt.Printf("Error removing %s", file)
			}
			file = fmt.Sprintf("raft-%d-snap", 1)
			if err := os.RemoveAll(file); err != nil {
				fmt.Printf("Error removing %s", file)
			}
		}

		expected, actual, err := f(c)

		log.Printf("[%v][%v][%v]", expected, actual, err)

		if err != nil {
			after(c)
			t.Fatalf("Error: %v", err)
		}

		if expected != actual {
			after(c)
			t.Fatalf("Expected %v but was %v", expected, actual)
		}

		after(c)
	})

	go c.Start()
}

func TestCreateTopic(t *testing.T) {
	fmt.Println("TestCreateTopic")
	f := func(c *Cluster) (interface{}, interface{}, error) {
		topicName := "test1"
		expected := c.CreateTopic(topicName)
		actual := c.Topics[topicName]
		return expected, actual, nil
	}

	clusterTest(t, f)
}

func TestAddDatabase(t *testing.T) {
	fmt.Println("TestAddDatabase")

	f := func(c *Cluster) (interface{}, interface{}, error) {
		name := "foo"
		database := types.NewSQLDB("", "")
		c.CreateDatabase(name, database)
		actual := c.Databases[name]
		return database, actual, nil
	}

	clusterTest(t, f)
}

func TestAppendToTopic(t *testing.T) {
	fmt.Println("TestAppendToTopic")
	f := func(c *Cluster) (interface{}, interface{}, error) {
		expected := util.Proposal{Topic: "test1", Proposal: "Hello World"}
		c.Append(expected)
		time.Sleep(2 * time.Second)

		lastIndex, err := c.storage.LastIndex()
		if err != nil {
			t.Fatalf("Error: %v", err)
		}

		entries, err := c.storage.Entries(lastIndex, lastIndex+1, 1)
		if err != nil {
			t.Fatalf("Error: %v", err)
		}

		actual, err := decodeProposal(entries[0].Data)

		return expected, actual, err
	}

	clusterTest(t, f)
}

type testReturn struct {
	Key string
	One string
}

var _ = Describe("Cluster", func() {
	clusterTest := func(f func(*Cluster) (expected interface{}, actual interface{}, err error)) {
		c := NewCluster([]string{"http://127.0.0.1:12379"}, 1, false)

		time.AfterFunc(2*time.Second, func() {
			log.Printf("Starting test function")
			after := func(c *Cluster) {
				err := c.Shutdown()
				Expect(err).To(BeNil())
				file := fmt.Sprintf("raft-%d", 1)
				err = os.RemoveAll(file)
				Expect(err).To(BeNil())
				file = fmt.Sprintf("raft-%d-snap", 1)
				err = os.RemoveAll(file)
				Expect(err).To(BeNil())
			}

			expected, actual, err := f(c)
			after(c)

			Expect(err).To(BeNil())
			Expect(expected).To(Equal(actual))
		})

		go c.Start()
	}

	Describe("Cluster", func() {
		var (
			data []byte
			err  error
		)

		JustBeforeEach(func() {
			data, err = ioutil.ReadFile("../syncable/simple.toml")
			Expect(err).To(BeNil())
		})

		It("should correctly add a Syncable to the cluster", func() {
			f := func(c *Cluster) (interface{}, interface{}, error) {
				name, sync, err := syncable.Parse("toml", bytes.NewReader(data), c.Databases)
				Expect(err).To(BeNil())

				err = c.CreateSyncable(name, sync)
				Expect(err).To(BeNil())

				time.Sleep(2 * time.Second)

				sqlSyncable, ok := sync.(*syncable.SQLSyncable)
				Expect(ok).To(BeTrue())

				e2 := reflect.ValueOf(sqlSyncable).Elem()
				db := e2.FieldByName("DB").Interface().(*sql.DB)
				defer db.Close()

				execInTransaction(db, "CREATE TABLE foo (key string, two string);")

				proposal := util.Proposal{Topic: "test1", Proposal: "{\"Key\": \"lock\", \"One\": \"two\"}"}
				c.Append(proposal)

				time.Sleep(2 * time.Second)

				expected := testReturn{Key: "lock", One: "two"}
				var actual testReturn
				err = SelectOneRowFromDB(db, "SELECT * FROM foo", &actual.Key, &actual.One)

				return expected, actual, err
			}

			clusterTest(f)
		})
	})
})

// func TestAddSQLSyncableToCluster(t *testing.T) {
// 	fmt.Println("TestAddSQLSyncableToCluster")
// 	f := func(c *Cluster) (interface{}, interface{}, error) {
// 		dat, err := ioutil.ReadFile("../syncable/simple.toml")
// 		if err != nil {
// 			return nil, nil, err
// 		}

// 		name, syncable, err := syncable.Parse("toml", bytes.NewReader(dat), c.Databases)
// 		if err != nil {
// 			return nil, nil, err
// 		}

// 		err = c.CreateSyncable(name, syncable)
// 		if err != nil {
// 			return nil, nil, err
// 		}

// 		time.Sleep(2 * time.Second)

// 		sqlSyncable, ok := syncable.(*syncable.SQLSyncable)
// 		if !ok {
// 			return nil, nil, err
// 		}
// 		e2 := reflect.ValueOf(sqlSyncable).Elem()
// 		db := e2.FieldByName("DB").Interface().(*sql.DB)
// 		defer db.Close()

// 		execInTransaction(db, "CREATE TABLE foo (key string, two string);", t)

// 		proposal := util.Proposal{Topic: "test1", Proposal: "{\"Key\": \"lock\", \"One\": \"two\"}"}
// 		c.Append(proposal)

// 		time.Sleep(2 * time.Second)

// 		expected := testReturn{Key: "lock", One: "two"}
// 		var actual testReturn
// 		err = SelectOneRowFromDB(db, "SELECT * FROM foo", &actual.Key, &actual.One)

// 		return expected, actual, err
// 	}

// 	clusterTest(t, f)
// }

func execInTransaction(db *sql.DB, sqlString string) {
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: 0, ReadOnly: false})
	Expect(err).To(BeNil())

	_, err = tx.ExecContext(context.Background(), sqlString)
	Expect(err).To(BeNil())

	err = tx.Commit()
	Expect(err).To(BeNil())
}

func SelectOneRowFromDB(db *sql.DB, table string, dest ...interface{}) error {
	rows, err := db.Query("SELECT * FROM foo")
	if err != nil {
		return err
	}
	defer rows.Close()
	var rowCount int
	for rows.Next() {
		rowCount++
		if err := rows.Scan(dest...); err != nil {
			return err
		}
	}
	if rowCount != 1 {
		return errors.New("Data is not in the database")
	}

	return rows.Err()
}
