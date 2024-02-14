package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"net.donbass.ipif2db/pkg/storage"
	// "net.donbass.ipif2db/pkg/storage"
)

const sqlTagName = "sql"

type DB struct {
	*sql.DB
	Log *func(str string, v ...interface{})
}

func NewClient(ctx context.Context, host, username, passwrord, database, dsnParams string,
	maxIdle, maxOpen, maxLifeTime int, logFunc *func(str string, v ...interface{})) (d *DB, err error) {

	pool, err := sql.Open("mysql",
		fmt.Sprintf("%s:%s@%s/%s%s", username, passwrord, host, database, dsnParams))
	if err != nil {
		return d, fmt.Errorf("failed connect to DB, err: %s", err)
	}

	pool.SetMaxIdleConns(maxIdle)
	pool.SetMaxOpenConns(maxOpen)
	pool.SetConnMaxLifetime(time.Duration(maxLifeTime) * time.Second)

	d = &DB{pool, logFunc}

	if err := d.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed ping DB: %s", err.Error())
	}

	return d, nil
}

func (d *DB) Disconnect() error {
	if err := d.Close(); err != nil {
		return fmt.Errorf("failed to disconnect from mysql: %s", err.Error())
	}
	return nil
}

// TODO NEED REFACTOR !!!
func (d *DB) Query(ctx context.Context, query string, args ...interface{}) (int64, error) {

	(*d.Log)("query: %s, %v", query, args)

	result, err := d.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("can't do query row in DB: %v", err)
	}

	return result.LastInsertId()
}

func (d *DB) SelectOne(ctx context.Context, v interface{}, query string, args ...interface{}) (err error) {

	rval := reflect.ValueOf(v)

	switch rval.Kind() {
	case reflect.Ptr:
		if rval.IsNil() {
			return errors.New("can't decode to nil value")
		}
		if rval = rval.Elem(); rval.Kind() != reflect.Struct {
			return fmt.Errorf("argument to decode must be a pointer to struct or a map, but got %v", rval)
		}

	case reflect.Map:
		if rval.IsNil() {
			return errors.New("can't decode to nil value")
		}
		fmt.Println("2")
	default:
		return fmt.Errorf("argument to decode must be a pointer to struct or a map, but got %v", rval)

	}

	(*d.Log)("select one: %s, %v", query, args)

	var rows *sql.Rows
	var columns []string
	numField := rval.Type().NumField()
	fieldPtrs := make([]any, 0, numField)

	rows, err = d.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return storage.ErrNoRow
	}

	if columns, err = rows.Columns(); err != nil {
		return fmt.Errorf("can't get column names from result query: %v", err)
	}

LOOP_SEARCH_PTR:
	for _, col := range columns {
		for i := 0; i < numField; i++ {
			if rval.Type().Field(i).Tag.Get(sqlTagName) == col {
				fieldPtrs = append(fieldPtrs, rval.Field(i).Addr().Interface())
				continue LOOP_SEARCH_PTR
			}
		}

		return fmt.Errorf("can't search field's ptr for row: %s", col)
	}

	if err = rows.Scan(fieldPtrs...); err != nil {
		return err
	}

	return nil
}

func (d *DB) SelectMany(ctx context.Context, v interface{}, query string, args ...interface{}) (err error) {

	resultsVal := reflect.ValueOf(v)
	if resultsVal.Kind() != reflect.Ptr {
		return fmt.Errorf("results argument must be a pointer to a slice, but was a %s", resultsVal.Kind())
	}

	sliceVal := resultsVal.Elem()
	if sliceVal.Kind() == reflect.Interface {
		sliceVal = sliceVal.Elem()
	}

	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("results argument must be a pointer to a slice, but was a pointer to %s", sliceVal.Kind())
	}

	(*d.Log)("select many: %s", query, args)

	var rows *sql.Rows
	if rows, err = d.QueryContext(ctx, query, args...); err != nil {
		return err
	}
	defer rows.Close()

	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return fmt.Errorf("can't get column names from result query: %v", err)
	}

	elemType := sliceVal.Type().Elem()
	fieldPtrs := make([]any, len(columns))
	fieldIndexes := make([]int, 0, len(columns))

LOOP_SEARCH_PTR:
	for _, col := range columns {
		for i := 0; i < elemType.NumField(); i++ {
			if elemType.Field(i).Tag.Get(sqlTagName) == col {
				fieldIndexes = append(fieldIndexes, i)
				continue LOOP_SEARCH_PTR
			}
		}

		return fmt.Errorf("can't search field's ptr for row: %s", col)
	}

	var index int
	for rows.Next() {
		newElem := reflect.New(elemType)
		sliceVal = reflect.Append(sliceVal, newElem.Elem())

		for i, fNum := range fieldIndexes {
			fieldPtrs[i] = sliceVal.Index(index).Field(fNum).Addr().Interface()
		}

		if err = rows.Scan(fieldPtrs...); err != nil {
			return err
		}
		index++
	}

	resultsVal.Elem().Set(sliceVal.Slice(0, index))
	return nil
}
