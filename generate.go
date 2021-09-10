package gocassa

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// CREATE TABLE users (
//   user_name varchar PRIMARY KEY,
//   password varchar,
//   gender varchar,
//   session_token varchar,
//   state varchar,
//   birth_year bigint
// );
//
// CREATE TABLE emp (
//   empID int,
//   deptID int,
//   first_name varchar,
//   last_name varchar,
//   PRIMARY KEY (empID, deptID)
// );
//

func createTableIfNotExist(keySpace string, types map[string]string, cf string, partitionKeys, colKeys []string, fields []string, values []interface{}, order []ClusteringOrderColumn, compoundKey, compact bool, compressor string) (string, error) {
	return createTableStmt("CREATE TABLE IF NOT EXISTS", keySpace, types, cf, partitionKeys, colKeys, fields, values, order, compoundKey, compact, compressor)
}

func createTable(keySpace string, types map[string]string, cf string, partitionKeys, colKeys []string, fields []string, values []interface{}, order []ClusteringOrderColumn, compoundKey, compact bool, compressor string) (string, error) {
	return createTableStmt("CREATE TABLE", keySpace, types, cf, partitionKeys, colKeys, fields, values, order, compoundKey, compact, compressor)
}

func createTableStmt(createStmt, keySpace string, types map[string]string, cf string, partitionKeys, colKeys []string, fields []string, values []interface{}, order []ClusteringOrderColumn, compoundKey, compact bool, compressor string) (string, error) {
	firstLine := fmt.Sprintf("%s %v.%v (", createStmt, keySpace, cf)
	fieldLines := []string{}
	for i := range fields {
		typeStr, err := stringTypeOf(types, values[i])
		if err != nil {
			return "", err
		}
		l := "    " + strings.ToLower(fields[i]) + " " + typeStr
		fieldLines = append(fieldLines, l)
	}
	//key generation
	str := ""
	if len(colKeys) > 0 { //key (or composite key) + clustering columns
		str = "    PRIMARY KEY ((%v), %v)"
	} else if compoundKey { //compound key just one set of parenthesis
		str = "    PRIMARY KEY (%v %v)"
	} else { //otherwise is a composite key without colKeys
		str = "    PRIMARY KEY ((%v %v))"
	}

	fieldLines = append(fieldLines, fmt.Sprintf(str, j(partitionKeys), j(colKeys)))

	lines := []string{
		firstLine,
		strings.Join(fieldLines, ",\n"),
		")",
	}

	if len(order) > 0 {
		orderStrs := make([]string, len(order))
		for i, o := range order {
			orderStrs[i] = fmt.Sprintf("%v %v", o.Column, o.Direction.String())
		}
		orderLine := fmt.Sprintf("WITH CLUSTERING ORDER BY (%v)", strings.Join(orderStrs, ", "))
		lines = append(lines, orderLine)
	}

	if compact {
		compactLineStart := "WITH"
		if len(order) > 0 {
			compactLineStart = "AND"
		}
		compactLine := fmt.Sprintf("%v COMPACT STORAGE", compactLineStart)
		lines = append(lines, compactLine)
	}

	if len(compressor) > 0 {
		compressionLineStart := "WITH"
		if len(order) > 0 || compact {
			compressionLineStart = "AND"
		}
		compressionLine := fmt.Sprintf("%v compression = {'sstable_compression': '%v'}", compressionLineStart, compressor)
		lines = append(lines, compressionLine)
	}

	lines = append(lines, ";")
	stmt := strings.Join(lines, "\n")
	return stmt, nil
}

func createTypeIfNotExist(keySpace, cf string, fields []string, values []interface{}) (string, error) {
	return createTypeStmt("CREATE TYPE IF NOT EXISTS", keySpace, cf, fields, values)
}

func createType(keySpace, cf string, fields []string, values []interface{}) (string, error) {
	return createTypeStmt("CREATE TYPE", keySpace, cf, fields, values)
}

func createTypeStmt(createStmt, keySpace, cf string, fields []string, values []interface{}) (string, error) {
	firstLine := fmt.Sprintf("%s %v.%v (", createStmt, keySpace, cf)
	fieldLines := []string{}
	for i := range fields {
		typeStr, err := stringTypeOf(nil, values[i])
		if err != nil {
			return "", err
		}
		l := "    " + strings.ToLower(fields[i]) + " " + typeStr
		fieldLines = append(fieldLines, l)
	}

	lines := []string{
		firstLine,
		strings.Join(fieldLines, ",\n"),
		")",
	}

	lines = append(lines, ";")
	stmt := strings.Join(lines, "\n")
	return stmt, nil
}

func j(s []string) string {
	s1 := []string{}
	for _, v := range s {
		s1 = append(s1, strings.ToLower(v))
	}
	return strings.Join(s1, ", ")
}

func createKeyspace(keyspaceName string) string {
	return fmt.Sprintf("CREATE KEYSPACE \"%v\" WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'eu-west-1' : 3}", keyspaceName)
}

func cassaType(i interface{}) gocql.Type {
	switch i.(type) {
	case int, int32:
		return gocql.TypeInt
	case int64:
		return gocql.TypeBigInt
	case int8, int16, uint, uint8, uint16, uint32, uint64:
		return gocql.TypeVarint
	case string:
		return gocql.TypeVarchar
	case float32:
		return gocql.TypeFloat
	case float64:
		return gocql.TypeDouble
	case bool:
		return gocql.TypeBoolean
	case time.Time:
		return gocql.TypeTimestamp
	case gocql.UUID:
		return gocql.TypeUUID
	case []byte:
		return gocql.TypeBlob
	case Counter:
		return gocql.TypeCounter
	}

	// Fallback to using reflection if type not recognised
	typ := reflect.TypeOf(i)
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return gocql.TypeInt
	case reflect.Int64:
		return gocql.TypeBigInt
	case reflect.String:
		return gocql.TypeVarchar
	case reflect.Float32:
		return gocql.TypeFloat
	case reflect.Float64:
		return gocql.TypeDouble
	case reflect.Bool:
		return gocql.TypeBoolean
	}

	return gocql.TypeCustom
}

func stringTypeOf(types map[string]string, i interface{}) (string, error) {
	_, isByteSlice := i.([]byte)
	if !isByteSlice {
		// Check if we found a higher kinded type
		switch reflect.ValueOf(i).Kind() {
		case reflect.Slice:
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			ct := cassaType(elemVal)
			if ct == gocql.TypeCustom {
				if types != nil {
					if udt := stringUdtOf(types, elemVal, "list<frozen<%v>>"); udt != "" {
						return udt, nil
					}
				}
				return "", fmt.Errorf("Unsupported type %T", i)
			}
			return fmt.Sprintf("list<%v>", ct), nil
		case reflect.Map:
			keyVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Key())).Interface()
			keyCt := cassaType(keyVal)
			if keyCt == gocql.TypeCustom {
				return "", fmt.Errorf("Unsupported map key type %T", i)
			}
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			elemCt := cassaType(elemVal)
			if elemCt == gocql.TypeCustom {
				if types != nil {
					if udt := stringUdtOf(types, elemVal, "map<%v, frozen<%v>>", keyCt); udt != "" {
						return udt, nil
					}
				}
				return "", fmt.Errorf("Unsupported map value type %T", i)
			}
			return fmt.Sprintf("map<%v, %v>", keyCt, elemCt), nil
		}
	}
	ct := cassaType(i)
	if ct == gocql.TypeCustom {
		if types != nil {
			if udt := stringUdtOf(types, i, "frozen<%v>"); udt != "" {
				return udt, nil
			}
		}
		return "", fmt.Errorf("Unsupported type %T", i)
	}
	return cassaTypeToString(ct)
}

func stringUdtOf(types map[string]string, i interface{}, format string, a ...interface{}) string {
	t := reflect.ValueOf(i).Type().String()
	udt := types[t]
	if udt == "" {
		return ""
	}
	if a != nil {
		return fmt.Sprintf(format, udt, a)
	}
	return fmt.Sprintf(format, udt)
}

func cassaTypeToString(t gocql.Type) (string, error) {
	switch t {
	case gocql.TypeInt:
		return "int", nil
	case gocql.TypeBigInt:
		return "bigint", nil
	case gocql.TypeVarint:
		return "varint", nil
	case gocql.TypeVarchar:
		return "varchar", nil
	case gocql.TypeFloat:
		return "float", nil
	case gocql.TypeDouble:
		return "double", nil
	case gocql.TypeBoolean:
		return "boolean", nil
	case gocql.TypeTimestamp:
		return "timestamp", nil
	case gocql.TypeUUID:
		return "uuid", nil
	case gocql.TypeBlob:
		return "blob", nil
	case gocql.TypeCounter:
		return "counter", nil
	default:
		return "", errors.New("unkown cassandra type")
	}
}
