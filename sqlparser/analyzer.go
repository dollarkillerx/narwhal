// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package sqlparser

// analyzer.go contains utility analysis functions.

import (
	"fmt"
	"strings"

	"github.com/dollarkillerx/narwhal/sqltypes"
)

// GetDBName parses the specified DML and returns the
// db name if it was used to qualify the table name.
// It returns an error if parsing fails or if the statement
// is not a DML.
func GetDBName(sql string) (string, error) {
	statement, err := Parse(sql)
	if err != nil {
		return "", err
	}
	switch stmt := statement.(type) {
	case *Insert:
		return string(stmt.Table.Qualifier), nil
	case *Update:
		return string(stmt.Table.Qualifier), nil
	case *Delete:
		return string(stmt.Table.Qualifier), nil
	}
	return "", fmt.Errorf("statement '%s' is not a dml", sql)
}

//Get the database and table name
func GetDBTable(token string) (string, string) {
	if len(token) == 0 {
		return "", ""
	}

	vec := strings.SplitN(token, ".", 2)
	if len(vec) == 2 {
		return strings.Trim(vec[0], "`"), strings.Trim(vec[1], "`")
	} else {
		return "", strings.Trim(vec[0], "`")
	}
}

func GetInsertDBTable(token string) (string, string) {
	if len(token) == 0 {
		return "", ""
	}

	vec := strings.SplitN(token, ".", 2)
	if len(vec) == 2 {
		table := strings.Split(vec[1], "(")
		return strings.Trim(vec[0], "`"), strings.Trim(table[0], "`")
	} else {
		table := strings.Split(vec[0], "(")
		return "", strings.Trim(table[0], "`")
	}
}

// GetColName returns the column name, only if
// it's a simple expression. Otherwise, it returns "".
func GetColName(node Expr) string {
	if n, ok := node.(*ColName); ok {
		return string(n.Name)
	}
	return ""
}

// IsColName returns true if the ValExpr is a *ColName.
func IsColName(node ValExpr) bool {
	_, ok := node.(*ColName)
	return ok
}

// IsVal returns true if the ValExpr is a string, number or value arg.
// NULL is not considered to be a value.
func IsValue(node ValExpr) bool {
	switch node.(type) {
	case StrVal, NumVal, ValArg:
		return true
	}
	return false
}

// HasINCaluse returns true if an yof the conditions has an IN clause.
func HasINClause(conditions []BoolExpr) bool {
	for _, node := range conditions {
		if c, ok := node.(*ComparisonExpr); ok && c.Operator == AST_IN {
			return true
		}
	}
	return false
}

// IsSimpleTuple returns true if the ValExpr is a ValTuple that
// contains simple values.
func IsSimpleTuple(node ValExpr) bool {
	list, ok := node.(ValTuple)
	if !ok {
		// It's a subquery.
		return false
	}
	for _, n := range list {
		if !IsValue(n) {
			return false
		}
	}
	return true
}

// AsInterface converts the ValExpr to an interface. It converts
// ValTuple to []interface{}, ValArg to string, StrVal to sqltypes.String,
// NumVal to sqltypes.Numeric. Otherwise, it returns an error.
func AsInterface(node ValExpr) (interface{}, error) {
	switch node := node.(type) {
	case ValTuple:
		vals := make([]interface{}, 0, len(node))
		for _, val := range node {
			v, err := AsInterface(val)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return vals, nil
	case ValArg:
		return string(node), nil
	case StrVal:
		return sqltypes.MakeString(node), nil
	case NumVal:
		n, err := sqltypes.BuildNumeric(string(node))
		if err != nil {
			return nil, fmt.Errorf("type mismatch: %s", err)
		}
		return n, nil
	}
	return nil, fmt.Errorf("unexpected node %v", node)
}

// StringIn is a convenience function that returns
// true if str matches any of the values.
func StringIn(str string, values ...string) bool {
	for _, val := range values {
		if str == val {
			return true
		}
	}
	return false
}
