package gocassa

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type typeFactory interface {
	NewType(string, interface{}, map[string]interface{}) Type
}

type tableFactory interface {
	NewTable(string, interface{}, map[string]interface{}, Keys) Table
}

type k struct {
	qe           QueryExecutor
	name         string
	debugMode    bool
	types        map[string]string
	typeFactory  typeFactory
	tableFactory tableFactory
}

// Connect to a certain keyspace directly. Same as using Connect().KeySpace(keySpaceName)
func ConnectToKeySpace(keySpace string, nodeIps []string, username, password string) (KeySpace, error) {
	c, err := Connect(nodeIps, username, password)
	if err != nil {
		return nil, err
	}
	return c.KeySpace(keySpace), nil
}

func (k *k) DebugMode(b bool) {
	k.debugMode = b
}

func (k *k) Type(name string, entity interface{}) Type {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	k.types[reflect.ValueOf(entity).Type().String()] = name
	return k.NewType(name, entity, m)
}

func (k *k) NewType(name string, entity interface{}, fields map[string]interface{}) Type {
	// Act both as a proxy to a tableFactory, and as the tableFactory itself (in most situations, a k will be its own
	// tableFactory, but not always [ie. mocking])
	if k.typeFactory != k {
		return k.typeFactory.NewType(name, entity, fields)
	} else {
		ti := newTypeInfo(k.name, name, entity, fields)
		return &udt{
			keySpace: k,
			info:     ti,
		}
	}
}

func (k *k) Table(name string, entity interface{}, keys Keys) Table {
	n := name + "__" + strings.Join(keys.PartitionKeys, "_") + "__" + strings.Join(keys.ClusteringColumns, "_")
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return k.NewTable(n, entity, m, keys)
}

func (k *k) NewTable(name string, entity interface{}, fields map[string]interface{}, keys Keys) Table {
	// Act both as a proxy to a tableFactory, and as the tableFactory itself (in most situations, a k will be its own
	// tableFactory, but not always [ie. mocking])
	if k.tableFactory != k {
		return k.tableFactory.NewTable(name, entity, fields, keys)
	} else {
		ti := newTableInfo(k.name, name, keys, entity, fields)
		return &t{
			keySpace: k,
			info:     ti,
			options:  Options{},
		}
	}
}

func (k *k) MapTable(name, id string, row interface{}) MapTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	return &mapT{
		Table: k.NewTable(fmt.Sprintf("%s_map_%s", name, id), row, m, Keys{
			PartitionKeys: []string{id},
		}),
		idField: id,
	}
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) MultimapTable(name, fieldToIndexBy, id string, row interface{}) MultimapTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	return &multimapT{
		Table: k.NewTable(fmt.Sprintf("%s_multimap_%s_%s", name, fieldToIndexBy, id), row, m, Keys{
			PartitionKeys:     []string{fieldToIndexBy},
			ClusteringColumns: []string{id},
		}),
		idField:        id,
		fieldToIndexBy: fieldToIndexBy,
	}
}

func (k *k) MultimapMultiKeyTable(name string, fieldToIndexBy, id []string, row interface{}) MultimapMkTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	return &multimapMkT{
		Table: k.NewTable(fmt.Sprintf("%s_multimapMk", name), row, m, Keys{
			PartitionKeys:     fieldToIndexBy,
			ClusteringColumns: id,
		}),
		idField:         id,
		fieldsToIndexBy: fieldToIndexBy,
	}
}

func (k *k) TimeSeriesTable(name, timeField, idField string, bucketSize time.Duration, row interface{}) TimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesT{
		Table: k.NewTable(fmt.Sprintf("%s_timeSeries_%s_%s_%s", name, timeField, idField, bucketSize), row, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}),
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

func (k *k) MultiTimeSeriesTable(name, indexField, timeField, idField string, bucketSize time.Duration, row interface{}) MultiTimeSeriesTable {
	return k.FlexMultiTimeSeriesTable(name, timeField, idField, []string{indexField}, &tsBucketer{bucketSize: bucketSize}, row)
}

func (k *k) FlexMultiTimeSeriesTable(name, timeField, idField string, indexFields []string, bucketer Bucketer, row interface{}) MultiTimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	pk := append([]string{}, indexFields...)
	pk = append(pk, bucketFieldName)
	return &multiTimeSeriesT{
		Table: k.NewTable(fmt.Sprintf("%s_multiTimeSeries_%s_%s_%s_%s", name, strings.Join(indexFields, "_"), timeField, idField, bucketer.String()), row, m, Keys{
			PartitionKeys:     pk,
			ClusteringColumns: []string{timeField, idField},
		}),
		indexFields: indexFields,
		timeField:   timeField,
		idField:     idField,
		bucketer:    bucketer,
	}
}

// Types returns type names in a keyspace
func (k *k) Types() ([]string, error) {
	const stmt = "SELECT type_name FROM system_schema.types WHERE keyspace_name = ?"
	maps, err := k.qe.Query(stmt, k.name)
	if err != nil {
		return nil, err
	}
	ret := []string{}
	for _, m := range maps {
		ret = append(ret, m["type_name"].(string))
	}
	return ret, nil
}

// Tables returns table names in a keyspace
func (k *k) Tables() ([]string, error) {
	const stmt = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
	maps, err := k.qe.Query(stmt, k.name)
	if err != nil {
		return nil, err
	}
	ret := []string{}
	for _, m := range maps {
		ret = append(ret, m["table_name"].(string))
	}
	return ret, nil
}

func (k *k) ExistsType(udt string) (bool, error) {
	ts, err := k.Types()
	if err != nil {
		return false, err
	}
	for _, v := range ts {
		if strings.EqualFold(v, udt) {
			return true, nil
		}
	}
	return false, nil
}

func (k *k) DropType(udt string) error {
	stmt := fmt.Sprintf("DROP TYPE IF EXISTS %s.%s", k.name, udt)
	return k.qe.Execute(stmt)
}

func (k *k) ExistsTable(table string) (bool, error) {
	ts, err := k.Tables()
	if err != nil {
		return false, err
	}
	for _, v := range ts {
		if strings.EqualFold(v, table) {
			return true, nil
		}
	}
	return false, nil
}

func (k *k) DropTable(table string) error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", k.name, table)
	return k.qe.Execute(stmt)
}

func (k *k) Name() string {
	return k.name
}
