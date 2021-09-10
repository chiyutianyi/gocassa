package gocassa

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func createTypeIf(cs TypeChanger, tes *testing.T) {
	err := cs.Recreate()
	if err != nil {
		tes.Fatal(err)
	}
}

func TestCreateType(t *testing.T) {
	rand.Seed(time.Now().Unix())
	randy := rand.Int() % 100
	name := fmt.Sprintf("customer_type_%v", randy)
	cs := ns.Type(name, Customer{})
	createTypeIf(cs, t)
	validateTypeName(t, cs.(TypeChanger), fmt.Sprintf("customer_type_%v", randy))

	res, err := ns.(*k).Types()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("Not found ", len(res))
	}
}

type CustomerWithUdt struct {
	Customer

	Udt Customer
}

func TestCreateTableWithUdt(t *testing.T) {
	rand.Seed(time.Now().Unix())
	randy := rand.Int() % 100
	name := fmt.Sprintf("customer_%v", randy)
	ts := ns.Type("customer", Customer{})
	createTypeIf(ts, t)
	cs := ns.Table(name, CustomerWithUdt{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	createIf(cs, t)
	validateTableName(t, cs.(TableChanger), fmt.Sprintf("customer_%d__Id_Name__", randy))
	err := cs.Set(CustomerWithUdt{
		Customer: Customer{
			Id:   "1001",
			Name: "Joe",
		},
		Udt: Customer{
			Id:   "1001",
			Name: "Joe",
		},
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &[]CustomerWithUdt{}
	err = cs.Where(Eq("Id", "1001"), Eq("Name", "Joe")).Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) != 1 {
		t.Fatal("Not found ", len(*res))
	}
	if (*res)[0].Udt.Id == "" {
		t.Fatal("Udt set failed")
	}
	err = ns.(*k).DropTable(name)
	if err != nil {
		t.Fatal(err)
	}
}

func validateTypeName(t *testing.T, tl TypeChanger, expected string) bool {
	ok := tl.Name() == expected
	if !ok {
		t.Fatalf("Table name should be: %s and NOT: %s", expected, tl.Name())
	}
	return ok
}
