package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/dollarkillerx/narwhal/core/errors"
	"log"
	"math"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dollarkillerx/narwhal/backend"
	"github.com/dollarkillerx/narwhal/core/golog"
	"github.com/dollarkillerx/narwhal/core/hack"
	"github.com/dollarkillerx/narwhal/mysql"
	"github.com/dollarkillerx/narwhal/proxy/server"
	"github.com/dollarkillerx/narwhal/sqlparser"
)

const logo = `
 ███▄    █  ▄▄▄       ██▀███   █     █░ ██░ ██  ▄▄▄       ██▓    
 ██ ▀█   █ ▒████▄    ▓██ ▒ ██▒▓█░ █ ░█░▓██░ ██▒▒████▄    ▓██▒    
▓██  ▀█ ██▒▒██  ▀█▄  ▓██ ░▄█ ▒▒█░ █ ░█ ▒██▀▀██░▒██  ▀█▄  ▒██░    
▓██▒  ▐▌██▒░██▄▄▄▄██ ▒██▀▀█▄  ░█░ █ ░█ ░▓█ ░██ ░██▄▄▄▄██ ▒██░    
▒██░   ▓██░ ▓█   ▓██▒░██▓ ▒██▒░░██▒██▓ ░▓█▒░██▓ ▓█   ▓██▒░██████▒
░ ▒░   ▒ ▒  ▒▒   ▓▒█░░ ▒▓ ░▒▓░░ ▓░▒ ▒   ▒ ░░▒░▒ ▒▒   ▓▒█░░ ▒░▓  ░
░ ░░   ░ ▒░  ▒   ▒▒ ░  ░▒ ░ ▒░  ▒ ░ ░   ▒ ░▒░ ░  ▒   ▒▒ ░░ ░ ▒  ░
   ░   ░ ░   ░   ▒     ░░   ░   ░   ░   ░  ░░ ░  ░   ▒     ░ ░   
         ░       ░  ░   ░         ░     ░  ░  ░      ░  ░    ░  ░
`

var baseConnId uint32 = 10000
var user string = "root"
var password string = "root"
var defaultConn *backend.DB
var nstring = sqlparser.String
var paramFieldData []byte
var columnFieldData []byte

func main() {
	fmt.Print(logo)
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.SetFlags(log.LstdFlags | log.Llongfile)

	// 注意 用户验证只支持mysql_native_password
	// ALTER USER 'yourusername'@'localhost' IDENTIFIED WITH mysql_native_password BY 'youpassword';
	open, err := backend.Open("192.168.88.11:3306", "jumpserver", "jumpserver", "crm", 10)
	if err != nil {
		log.Fatalln(err)
	}

	err = open.Ping()
	if err != nil {
		log.Fatalln(err)
	}
	defaultConn = open

	server := CoreServer{db: open, counter: new(server.Counter)}
	server.Run()
}

type CoreServer struct {
	db *backend.DB

	statusIndex        int32
	status             [2]int32
	logSqlIndex        int32
	logSql             [2]string
	slowLogTimeIndex   int32
	slowLogTime        [2]int
	blacklistSqlsIndex int32

	counter *server.Counter
}

func (c *CoreServer) Run() {
	listen, err := net.Listen("tcp", "0.0.0.0:8675")
	if err != nil {
		log.Fatalln(err)
	}

	for {
		accept, err := listen.Accept()
		if err != nil {
			log.Println(err)
			return
		}

		go c.core(accept)
	}
}

func (c *CoreServer) core(conn net.Conn) {
	clientConn := c.newClientConn(conn)
	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			log.Println("server", "onConn", "error", 0,
				"remoteAddr", conn.RemoteAddr().String(),
				"stack", string(buf),
			)
		}

		err = clientConn.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	err := clientConn.Handshake()
	if err != nil {
		log.Println(err)

		er2 := clientConn.writeError(err)
		if er2 != nil {
			log.Println(er2)
		}

		er2 = clientConn.Close()
		if er2 != nil {
			log.Println(er2)
		}
		return
	}

	clientConn.Run()
}

// ========================== client conn
func (c *CoreServer) newClientConn(co net.Conn) *ClientConn {
	conn := new(ClientConn)
	tcpConn := co.(*net.TCPConn)
	tcpConn.SetNoDelay(false) // nagle false
	conn.conn = tcpConn
	conn.pkg = mysql.NewPacketIO(tcpConn)
	conn.connectionId = atomic.AddUint32(&baseConnId, 1)
	conn.status = mysql.SERVER_STATUS_AUTOCOMMIT
	conn.salt, _ = mysql.RandomBuf(20)

	conn.charset = mysql.DEFAULT_CHARSET
	conn.collation = mysql.DEFAULT_COLLATION_ID

	conn.stmtId = 0
	conn.stmts = make(map[uint32]*server.Stmt)
	conn.proxy = c

	return conn
}

type ClientConn struct {
	conn   net.Conn
	pkg    *mysql.PacketIO
	closed bool

	connectionId uint32
	status       uint16
	salt         []byte
	capability   uint32

	user string
	db   string

	charset   string
	collation mysql.CollationId

	stmtId    uint32
	stmts     map[uint32]*server.Stmt //prepare相关,client端到proxy的stmt
	configVer uint32                  //check config version for reload online

	affectedRows int64

	proxy *CoreServer
}

func (c *ClientConn) Close() error {
	if c.closed {
		return nil
	}

	c.conn.Close()

	c.closed = true

	return nil
}

func (c *ClientConn) Handshake() error {
	// 1. 建立基础链接
	err := c.writeInitialHandshake()
	if err != nil {
		log.Println(err)
		return err
	}

	// 2. 用户认证
	err = c.readHandshakeResponse()
	if err != nil {
		log.Println(err)
		return err
	}

	if err := c.writeOK(nil); err != nil {
		log.Println("server", "readHandshakeResponse",
			"write ok fail",
			c.connectionId, "error", err.Error())
		return err
	}

	c.pkg.Sequence = 0

	return nil
}

func (c *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(server.DEFAULT_CAPABILITY), byte(server.DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(server.DEFAULT_CAPABILITY>>16), byte(server.DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return c.writePacket(data)
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return c.pkg.WritePacketBatch(total, data, direct)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacketBatch(total, data, direct)
}

func (c *ClientConn) readHandshakeResponse() error {
	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	//check user
	if c.user != user {
		log.Println("No User")
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.conn.RemoteAddr().String(), "Yes")
	}

	//check password
	checkAuth := mysql.CalcPassword(c.salt, []byte(password))
	if !bytes.Equal(auth, checkAuth) {
		log.Println("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"client_user", c.user,
			"config_set_user", c.user,
			"password", password)
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.conn.RemoteAddr().String(), "Yes")
	}

	pos += authLen

	var db string
	if c.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(c.db) + 1

	}
	c.db = db

	return nil
}

func (c *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *ClientConn) Run() {
	//defer c.clean() TODO: 连接池逃逸检查

	for {
		packet, err := c.readPacket()
		if err != nil {
			log.Println(err)
			return
		}

		if err = c.dispatch(packet); err != nil {
			c.writeError(err)
			log.Println("ClientConn", "Run",
				err.Error(), c.connectionId,
			)
			if err == mysql.ErrBadConn {
				c.Close()
			}
			return
		}

		if c.closed {
			return
		}

		c.pkg.Sequence = 0
	}
}

func (c *ClientConn) handleRollback() (err error) {
	if err := c.rollback(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
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

func (c *ClientConn) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	delete(c.stmts, id)

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
			pos += paramNum << 1

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

func (c *ClientConn) bindStmtArgs(s *server.Stmt, nullBitmap, paramTypes, paramValues []byte) error {
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

func (c *ClientConn) closeConn(conn *backend.BackendConn, rollback bool) {
	if c.isInTransaction() {
		return
	}
	defer conn.Close()
	if rollback {
		conn.Rollback()
	}
}

func (c *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.SelectExprs))

	for i, expr := range stmt.SelectExprs {
		r.Fields[i] = &mysql.Field{}
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			r.Fields[i].Name = []byte("*")
		case *sqlparser.NonStarExpr:
			if e.As != nil {
				r.Fields[i].Name = e.As
				r.Fields[i].OrgName = hack.Slice(nstring(e.Expr))
			} else {
				r.Fields[i].Name = hack.Slice(nstring(e.Expr))
			}
		default:
			r.Fields[i].Name = hack.Slice(nstring(e))
		}
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

func (c *ClientConn) writeResultset(status uint16, r *mysql.Resultset) error {
	c.affectedRows = int64(-1)
	total := make([]byte, 0, 4096)
	data := make([]byte, 4, 512)
	var err error

	columnLen := mysql.PutLengthEncodedInt(uint64(len(r.Fields)))

	data = append(data, columnLen...)
	total, err = c.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	for _, v := range r.Fields {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	total, err = c.writeEOFBatch(total, status, false)
	if err != nil {
		return err
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	total, err = c.writeEOFBatch(total, status, true)
	total = nil
	if err != nil {
		return err
	}

	return nil
}

func (c *ClientConn) executeInNode(conn *backend.BackendConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var state string
	startTime := time.Now().UnixNano()
	r, err := conn.Execute(sql, args...)
	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}
	execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
	if strings.ToLower(c.proxy.logSql[c.proxy.logSqlIndex]) != golog.LogSqlOff &&
		execTime >= float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
		log.Println(state, "%.1fms - %s->%s:%s",
			execTime,
			c.conn.RemoteAddr(),
			conn.GetAddr(),
			sql,
		)
	}

	if err != nil {
		return nil, err
	}

	return []*mysql.Result{r}, err
}

func (c *ClientConn) handlePrepareSelect(stmt *sqlparser.Select, sql string, args []interface{}) error {
	// 链接到数据库
	conn, err := c.getBackendConn(true)
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
	conn, err := c.getBackendConn(true)
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

func (c *ClientConn) writePrepare(s *server.Stmt) error {
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

// TODO: 处理 rollback
func (c *ClientConn) rollback() (err error) {
	//c.status &= ^mysql.SERVER_STATUS_IN_TRANS
	//
	//for _, co := range c.txConns {
	//	if e := co.Rollback(); e != nil {
	//		err = e
	//	}
	//	co.Close()
	//}
	//
	//c.txConns = make(map[*backend.Node]*backend.BackendConn)
	return
}

func (c *ClientConn) dispatch(data []byte) error {
	cmd := data[0]
	data := data[1:]

	switch cmd {
	case mysql.COM_QUIT:
		c.handleRollback()
		c.Close()
		return nil
	case mysql.COM_QUERY:
		return c.handleQuery(hack.String(data))
	case mysql.COM_PING:
		return c.writeOK(nil)
	case mysql.COM_INIT_DB:
		return c.handleUseDB(hack.String(data))
	case mysql.COM_FIELD_LIST:
		return c.handleFieldList(data)
	case mysql.COM_STMT_PREPARE:
		return c.handleStmtPrepare(hack.String(data))
	case mysql.COM_STMT_EXECUTE:
		return c.handleStmtExecute(data)
	case mysql.COM_STMT_CLOSE:
		return c.handleStmtClose(data)
	case mysql.COM_STMT_SEND_LONG_DATA:
		return c.handleStmtSendLongData(data)
	case mysql.COM_STMT_RESET:
		return c.handleStmtReset(data)
	case mysql.COM_SET_OPTION:
		return c.writeEOF(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		log.Println("ClientConn", "dispatch", msg, 0)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}
}

// ==== tx

func (c *ClientConn) isInTransaction() bool {
	return c.status&mysql.SERVER_STATUS_IN_TRANS > 0 ||
		!c.isAutoCommit()
}

func (c *ClientConn) isAutoCommit() bool {
	return c.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

// ==== tx_end

func (c *ClientConn) getBackendConn(fromSlave bool) (co *backend.BackendConn, err error) {
	if !c.isInTransaction() {
		co, err = defaultConn.GetConn()
		if err != nil {
			log.Println("server", "getBackendConn", err.Error(), 0)
			return
		}
	} else {
		// 如果 是 事物 每一个 事物 建立一个链接
		co, err = defaultConn.GetConn()
		if err != nil {
			log.Println("server", "getBackendConn", err.Error(), 0)
			return
		}

		if !c.isAutoCommit() {
			err := co.SetAutoCommit(0)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		} else {
			err := co.Begin()
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}

		//var ok bool
		//co, ok = c.txConns[n]
		//
		//if !ok {
		//	if co, err = n.GetMasterConn(); err != nil {
		//		return
		//	}
		//
		//	if !c.isAutoCommit() {
		//		if err = co.SetAutoCommit(0); err != nil {
		//			return
		//		}
		//	} else {
		//		if err = co.Begin(); err != nil {
		//			return
		//		}
		//	}
		//
		//	c.txConns[n] = co
		//}
	}

	if err = co.UseDB(c.db); err != nil {
		//reset the database to null
		c.db = ""
		return
	}

	if err = co.SetCharset(c.charset, c.collation); err != nil {
		return
	}

	return
}

func (c *ClientConn) handleStmtPrepare(sql string) error {
	s := new(server.Stmt)

	sql = strings.TrimRight(sql, ";")

	var err error
	s.S, err = sqlparser.Parse(sql)
	if err != nil {
		return fmt.Errorf(`parse sql "%s" error`, sql)
	}

	s.Sql = sql

	conn, err := c.getBackendConn(true)
	defer c.closeConn(conn, false)
	if err != nil {
		return fmt.Errorf("prepare error %s", err)
	}

	err = conn.UseDB(c.db)
	if err != nil {
		//reset the database to null
		c.db = ""
		return fmt.Errorf("prepare error %s", err)
	}

	t, err := conn.Prepare(sql)
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

	err = conn.ClosePrepare(t.GetId())
	if err != nil {
		return err
	}

	return nil
}

// ========================== client conn end

// ============= select
func (c *ClientConn) handleFieldList(data []byte) error {
	index := bytes.IndexByte(data, 0x00)
	table := string(data[0:index])
	wildcard := string(data[index+1:])

	co, err := c.getBackendConn(true)
	defer c.closeConn(co, false)
	if err != nil {
		return err
	}

	if err = co.UseDB(c.db); err != nil {
		//reset the database to null
		c.db = ""
		return err
	}

	if fs, err := co.FieldList(table, wildcard); err != nil {
		return err
	} else {
		return c.writeFieldList(c.status, fs)
	}
}

func (c *ClientConn) writeFieldList(status uint16, fs []*mysql.Field) error {
	c.affectedRows = int64(-1)
	var err error
	total := make([]byte, 0, 1024)
	data := make([]byte, 4, 512)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	_, err = c.writeEOFBatch(total, status, true)
	return err
}

func (c *ClientConn) handleUseDB(dbName string) error {
	var co *backend.BackendConn
	var err error

	if len(dbName) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}

	co, err = c.getBackendConn(true)
	defer c.closeConn(co, false)
	if err != nil {
		return err
	}

	if err = co.UseDB(dbName); err != nil {
		//reset the client database to null
		c.db = ""
		return err
	}
	c.db = dbName
	return c.writeOK(nil)
}

func (c *ClientConn) handleQuery(sql string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			golog.OutputSql("Error", "err:%v,sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				golog.Error("ClientConn", "handleQuery",
					err.Error(), 0,
					"stack", string(buf), "sql", sql)
			}

			err = errors.ErrInternalServer
			return
		}
	}()

	sql = strings.TrimRight(sql, ";") //删除sql语句最后的分号
	hasHandled, err := c.preHandleShard(sql)
	if err != nil {
		golog.Error("server", "preHandleShard", err.Error(), 0,
			"sql", sql,
			"hasHandled", hasHandled,
		)
		return err
	}
	if hasHandled {
		return nil
	}

	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql) //解析sql语句,得到的stmt是一个interface
	if err != nil {
		golog.Error("server", "parse", err.Error(), 0, "hasHandled", hasHandled, "sql", sql)
		return err
	}

	switch v := stmt.(type) {
	case *sqlparser.Select:
		return c.handleSelect(v, nil)
	case *sqlparser.Insert:
		return c.handleExec(stmt, nil)
	case *sqlparser.Update:
		return c.handleExec(stmt, nil)
	case *sqlparser.Delete:
		return c.handleExec(stmt, nil)
	case *sqlparser.Replace:
		return c.handleExec(stmt, nil)
	case *sqlparser.Set:
		return c.handleSet(v, sql)
	case *sqlparser.Begin:
		return c.handleBegin()
	case *sqlparser.Commit:
		return c.handleCommit()
	case *sqlparser.Rollback:
		return c.handleRollback()
	case *sqlparser.Admin:
		if c.user == "root" {
			return c.handleAdmin(v)
		}
		return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.AdminHelp:
		if c.user == "root" {
			return c.handleAdminHelp(v)
		}
		return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.UseDB:
		return c.handleUseDB(v.DB)
	case *sqlparser.SimpleSelect:
		return c.handleSimpleSelect(v)
	case *sqlparser.Truncate:
		return c.handleExec(stmt, nil)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}

	return nil
}

//preprocessing sql before parse sql
func (c *ClientConn) preHandleShard(sql string) (bool, error) {
	var rs []*mysql.Result
	var err error
	var executeDB *server.ExecuteDB

	if len(sql) == 0 {
		return false, errors.ErrCmdUnsupport
	}
	//filter the blacklist sql
	if c.proxy.blacklistSqls[c.proxy.blacklistSqlsIndex].sqlsLen != 0 {
		if c.isBlacklistSql(sql) {
			log.Println("Forbidden", "%s->%s:%s",
				c.conn.RemoteAddr(),
				sql,
			)
			err := mysql.NewError(mysql.ER_UNKNOWN_ERROR, "sql in blacklist.")
			return false, err
		}
	}

	tokens := strings.FieldsFunc(sql, hack.IsSqlSep)

	if len(tokens) == 0 {
		return false, errors.ErrCmdUnsupport
	}

	if c.isInTransaction() {
		executeDB, err = c.GetTransExecDB(tokens, sql)
	} else {
		executeDB, err = c.GetExecDB(tokens, sql)
	}

	if err != nil {
		//this SQL doesn't need execute in the backend.
		if err == errors.ErrIgnoreSQL {
			err = c.writeOK(nil)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	//need shard sql
	if executeDB == nil {
		return false, nil
	}
	//get connection in DB
	conn, err := c.getBackendConn(executeDB.IsSlave)
	defer c.closeConn(conn, false)
	if err != nil {
		return false, err
	}
	//execute.sql may be rewritten in getShowExecDB
	rs, err = c.executeInNode(conn, executeDB.sql, nil)
	if err != nil {
		return false, err
	}

	if len(rs) == 0 {
		msg := fmt.Sprintf("result is empty")
		golog.Error("ClientConn", "handleUnsupport", msg, 0, "sql", sql)
		return false, mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	c.lastInsertId = int64(rs[0].InsertId)
	c.affectedRows = int64(rs[0].AffectedRows)

	if rs[0].Resultset != nil {
		err = c.writeResultset(c.status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// ============= select end
