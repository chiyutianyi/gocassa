package gocassa

type udt struct {
	keySpace *k
	info     *typeInfo
}

// Contains mostly analyzed information about the entity
type typeInfo struct {
	keyspace, name string
	marshalSource  interface{}
	fieldSource    map[string]interface{}
	fieldNames     map[string]struct{} // This is here only to check containment
	fields         []string
	fieldValues    []interface{}
}

func newTypeInfo(keyspace, name string, entity interface{}, fieldSource map[string]interface{}) *typeInfo {
	cinf := &typeInfo{
		keyspace:      keyspace,
		name:          name,
		marshalSource: entity,
		fieldSource:   fieldSource,
	}
	fields := []string{}
	values := []interface{}{}
	for k, v := range fieldSource {
		fields = append(fields, k)
		values = append(values, v)
	}
	cinf.fieldNames = map[string]struct{}{}
	for _, v := range fields {
		cinf.fieldNames[v] = struct{}{}
	}
	cinf.fields = fields
	cinf.fieldValues = values
	return cinf
}

func (t *udt) Create() error {
	if stmt, err := t.CreateStatement(); err != nil {
		return err
	} else {
		return t.keySpace.qe.Execute(stmt)
	}
}

func (t *udt) CreateIfNotExist() error {
	if stmt, err := t.CreateIfNotExistStatement(); err != nil {
		return err
	} else {
		return t.keySpace.qe.Execute(stmt)
	}
}

func (t *udt) Recreate() error {
	if ex, err := t.keySpace.ExistsType(t.Name()); ex && err == nil {
		if err := t.keySpace.DropType(t.Name()); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return t.Create()
}

func (t *udt) CreateStatement() (string, error) {
	return createType(t.keySpace.name,
		t.Name(),
		t.info.fields,
		t.info.fieldValues,
	)
}

func (t *udt) CreateIfNotExistStatement() (string, error) {
	return createTypeIfNotExist(t.keySpace.name,
		t.Name(),
		t.info.fields,
		t.info.fieldValues,
	)
}

func (t *udt) Name() string {
	return t.info.name
}
