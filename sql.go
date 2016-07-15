package gosql

import (
	"database/sql"
	"errors"
	"strings"
	"sync"
)

//RowsCallback Rows数据结果回调函数
type RowsCallback func(rows *sql.Rows)

//TransactionFunc 事务回调函数
type TransactionFunc func(tx *sql.Tx) error

//DbConnect 数据库连接结构接口
type DbConnect interface {
	Create(connect string) (SQL, error)
}

var (
	driversMu sync.Mutex
	drivers   = make(map[string]DbConnect)
)

//SQL 数据库操作接口
type SQL interface {
	//RowsCallbackResult 查询多条数据,结果以回调函数处理
	//param sql string SQL
	//param callback func(*sql.Rows) 回调函数指针
	//param args... interface{} SQL参数
	RowsCallbackResult(sql string, callback RowsCallback, args ...interface{}) error
	//Rows 查询多条数据,结果以[]map[string]interface{}方式返回
	//返回结果,使用本package中的类型函数进行数据解析
	//eg:
	//		result := Rows(...)
	//		for _,mp := range result {
	//			Int(mp["colum"])
	//			String(mp["colum"])
	//			.......
	//		}
	//param sql string SQL
	//param args... interface{} SQL参数
	Rows(sql string, args ...interface{}) ([]map[string]interface{}, error)
	//Row 查询单条语句,返回结果
	//param sql string SQL
	//param args... interface{} SQL参数
	Row(sql string, args ...interface{}) (*sql.Row, error)
	//Exec 执行一条SQL
	//param sql string SQL
	//param args... interface{} SQL参数
	Exec(sql string, args ...interface{}) (sql.Result, error)
	//Count SQL语句条数统计
	//param sql string SQL
	//param args... interface{} SQL参数
	Count(sql string, args ...interface{}) (int64, error)
	//ParseSQL 解析SQL
	//param sql string SQL
	//param args map[string]interface{} 参数映射
	ParseSQL(sql string, args map[string]interface{}) (string, []interface{}, error)
	//Transaction 事务处理
	//param t TransactionFunc 事务处理函数
	Transaction(t TransactionFunc) error
	//GetDb 获取数据库对象
	GetDb() (*sql.DB, error)
	//Close 关闭数据库
	Close()
}

//Register 注册数据库操作对象
func Register(name string, driver DbConnect) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("sql: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

//Open 打开数据库连接
func Open(connectString string) (SQL, error) {
	strs := strings.Split(connectString, "://")
	if strs == nil || len(strs) < 2 {
		return nil, errors.New("数据库连接字符串异常，连接失败")
	}
	driversMu.Lock()
	if value, ok := drivers[strs[0]]; ok {
		driversMu.Unlock()
		return value.Create(strs[1])
	}
	driversMu.Unlock()
	return nil, errors.New("未注册数据库[" + strs[0] + "]操作对象")
}
