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

package server

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dollarkillerx/narwhal/core/errors"
	"github.com/dollarkillerx/narwhal/core/golog"
	"github.com/dollarkillerx/narwhal/mysql"
	"github.com/dollarkillerx/narwhal/sqlparser"
)

var paramFieldData []byte
var columnFieldData []byte

func init() {
	var p = &mysql.Field{Name: []byte("?")}
	var c = &mysql.Field{}

	paramFieldData = p.Dump()
	columnFieldData = c.Dump()
}

type Stmt struct {
	Id uint32

	Params  int
	Columns int

	Args []interface{}

	S sqlparser.Statement

	Sql string
}

func (s *Stmt) ResetParams() {
	s.Args = make([]interface{}, s.Params)
}

func (c *ClientConn) handleStmtPrepare(sql string) error {
	if c.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	s := new(Stmt)

	sql = strings.TrimRight(sql, ";")

	var err error
	s.S, err = sqlparser.Parse(sql)
	if err != nil {
		return fmt.Errorf(`parse sql "%s" error`, sql)
	}

	s.Sql = sql

	defaultRule := c.schema.rule.DefaultRule

	n := c.proxy.GetNode(defaultRule.Nodes[0])

	co, err := c.getBackendConn(n, false)
	defer c.closeConn(co, false)
	if err != nil {
		return fmt.Errorf("prepare error %s", err)
	}

	err = co.UseDB(c.db)
	if err != nil {
		//reset the database to null
		c.db = ""
		return fmt.Errorf("prepare error %s", err)
	}

	t, err := co.Prepare(sql)
	if err != nil {
		return fmt.Errorf("prepare error %s", err)
	}
	s.Params = t.ParamNum()
	s.Columns = t.ColumnNum()

	s.Id = c.stmtId
	c.stmtId++

	if err = c.writePrepare(s); err != nil {
		return err
	}

	s.ResetParams()
	c.stmts[s.Id] = s

	err = co.ClosePrepare(t.GetId())
	if err != nil {
		return err
	}

	return nil
}

func (c *ClientConn) writePrepare(s *Stmt) error {
	var err error
	data := make([]byte, 4, 128)
	total := make([]byte, 0, 1024)
	//status ok
	data = append(data, 0)
	//stmt id
	data = append(data, mysql.Uint32ToBytes(s.Id)...)
	//number columns
	data = append(data, mysql.Uint16ToBytes(uint16(s.Columns))...)
	//number params
	data = append(data, mysql.Uint16ToBytes(uint16(s.Params))...)
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0)

	total, err = c.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	if s.Params > 0 {
		for i := 0; i < s.Params; i++ {
			data = data[0:4]
			data = append(data, []byte(paramFieldData)...)

			total, err = c.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = c.writeEOFBatch(total, c.status, false)
		if err != nil {
			return err
		}
	}

	if s.Columns > 0 {
		for i := 0; i < s.Columns; i++ {
			data = data[0:4]
			data = append(data, []byte(columnFieldData)...)

			total, err = c.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = c.writeEOFBatch(total, c.status, false)
		if err != nil {
			return err
		}

	}
	total, err = c.writePacketBatch(total, nil, true)
	total = nil
	if err != nil {
		return err
	}
	return nil
}

func (c *ClientConn) handleStmtExecute(data []byte) error {
	if len(data) < 9 {
		return mysql.ErrMalformPacket
	}

	pos := 0
	id := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	s, ok := c.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_execute")
	}

	flag := data[pos]
	pos++
	//now we only support CURSOR_TYPE_NO_CURSOR flag
	if flag != 0 {
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("unsupported flag %d", flag))
	}

	//skip iteration-count, always 1
	pos += 4

	var nullBitmaps []byte
	var paramTypes []byte
	var paramValues []byte

	paramNum := s.Params

	if paramNum > 0 {
		nullBitmapLen := (s.Params + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return mysql.ErrMalformPacket
		}
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		//new param bound flag
		if data[pos] == 1 {
			pos++
			if len(data) < (pos + (paramNum << 1)) {
				return mysql.ErrMalformPacket
			}

			paramTypes = data[pos : pos+(paramNum<<1)]
			pos += (paramNum << 1)

			paramValues = data[pos:]
		}

		if err := c.bindStmtArgs(s, nullBitmaps, paramTypes, paramValues); err != nil {
			return err
		}
	}

	var err error

	switch stmt := s.S.(type) {
	case *sqlparser.Select:
		err = c.handlePrepareSelect(stmt, s.Sql, s.Args)
	case *sqlparser.Insert:
		err = c.handlePrepareExec(s.S, s.Sql, s.Args)
	case *sqlparser.Update:
		err = c.handlePrepareExec(s.S, s.Sql, s.Args)
	case *sqlparser.Delete:
		err = c.handlePrepareExec(s.S, s.Sql, s.Args)
	case *sqlparser.Replace:
		err = c.handlePrepareExec(s.S, s.Sql, s.Args)
	default:
		err = fmt.Errorf("command %T not supported now", stmt)
	}

	s.ResetParams()

	return err
}

func (c *ClientConn) handlePrepareSelect(stmt *sqlparser.Select, sql string, args []interface{}) error {
	defaultRule := c.schema.rule.DefaultRule
	if len(defaultRule.Nodes) == 0 {
		return errors.ErrNoDefaultNode
	}
	defaultNode := c.proxy.GetNode(defaultRule.Nodes[0])

	//choose connection in slave DB first
	conn, err := c.getBackendConn(defaultNode, true)
	defer c.closeConn(conn, false)
	if err != nil {
		return err
	}

	if conn == nil {
		r := c.newEmptyResultset(stmt)
		return c.writeResultset(c.status, r)
	}

	var rs []*mysql.Result
	rs, err = c.executeInNode(conn, sql, args)
	if err != nil {
		golog.Error("ClientConn", "handlePrepareSelect", err.Error(), c.connectionId)
		return err
	}

	status := c.status | rs[0].Status
	if rs[0].Resultset != nil {
		err = c.writeResultset(status, rs[0].Resultset)
	} else {
		r := c.newEmptyResultset(stmt)
		err = c.writeResultset(status, r)
	}

	return err
}

func (c *ClientConn) handlePrepareExec(stmt sqlparser.Statement, sql string, args []interface{}) error {
	defaultRule := c.schema.rule.DefaultRule
	if len(defaultRule.Nodes) == 0 {
		return errors.ErrNoDefaultNode
	}
	defaultNode := c.proxy.GetNode(defaultRule.Nodes[0])

	//execute in Master DB
	conn, err := c.getBackendConn(defaultNode, false)
	defer c.closeConn(conn, false)
	if err != nil {
		return err
	}

	if conn == nil {
		return c.writeOK(nil)
	}

	var rs []*mysql.Result
	rs, err = c.executeInNode(conn, sql, args)
	c.closeConn(conn, false)

	if err != nil {
		golog.Error("ClientConn", "handlePrepareExec", err.Error(), c.connectionId)
		return err
	}

	status := c.status | rs[0].Status
	if rs[0].Resultset != nil {
		err = c.writeResultset(status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	return err
}

func (c *ClientConn) bindStmtArgs(s *Stmt, nullBitmap, paramTypes, paramValues []byte) error {
	args := s.Args

	pos := 0

	var v []byte
	var n int = 0
	var isNull bool
	var err error

	for i := 0; i < s.Params; i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.MYSQL_TYPE_NULL:
			args[i] = nil
			continue

		case mysql.MYSQL_TYPE_TINY:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint8(paramValues[pos])
			} else {
				args[i] = int8(paramValues[pos])
			}

			pos++
			continue

		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			if len(paramValues) < (pos + 2) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			} else {
				args[i] = int16((binary.LittleEndian.Uint16(paramValues[pos : pos+2])))
			}
			pos += 2
			continue

		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			} else {
				args[i] = int32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case mysql.MYSQL_TYPE_LONGLONG:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			} else {
				args[i] = int64(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			}
			pos += 8
			continue

		case mysql.MYSQL_TYPE_FLOAT:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			args[i] = float32(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case mysql.MYSQL_TYPE_DOUBLE:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VARCHAR,
			mysql.MYSQL_TYPE_BIT, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_TINY_BLOB,
			mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB,
			mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_GEOMETRY,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIME:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			v, isNull, n, err = mysql.LengthEnodedString(paramValues[pos:])
			pos += n
			if err != nil {
				return err
			}

			if !isNull {
				args[i] = v
				continue
			} else {
				args[i] = nil
				continue
			}
		default:
			return fmt.Errorf("Stmt Unknown FieldType %d", tp)
		}
	}
	return nil
}

func (c *ClientConn) handleStmtSendLongData(data []byte) error {
	if len(data) < 6 {
		return mysql.ErrMalformPacket
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := c.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_send_longdata")
	}

	paramId := binary.LittleEndian.Uint16(data[4:6])
	if paramId >= uint16(s.Params) {
		return mysql.NewDefaultError(mysql.ER_WRONG_ARGUMENTS, "stmt_send_longdata")
	}

	if s.Args[paramId] == nil {
		s.Args[paramId] = data[6:]
	} else {
		if b, ok := s.Args[paramId].([]byte); ok {
			b = append(b, data[6:]...)
			s.Args[paramId] = b
		} else {
			return fmt.Errorf("invalid param long data type %T", s.Args[paramId])
		}
	}

	return nil
}

func (c *ClientConn) handleStmtReset(data []byte) error {
	if len(data) < 4 {
		return mysql.ErrMalformPacket
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := c.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_reset")
	}

	s.ResetParams()

	return c.writeOK(nil)
}

func (c *ClientConn) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	delete(c.stmts, id)

	return nil
}
