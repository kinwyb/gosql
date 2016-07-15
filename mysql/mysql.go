package mysql

import (
	"database/sql"
	"errors"
	"regexp"

	"github.com/kinwyb/gosql"
)

var (
	//ErrorNotOpen 数据库打开失败
	ErrorNotOpen = errors.New("数据库连接失败")
)

func init() {
	gosql.Register("mysql", &Conn{})
}

//Conn mysql数据库连接对象
type Conn struct {
}

//Create 打开一个数据库连接
func (c *Conn) Create(connect string) gosql.SQL {
	result := &Mysql{}
	result.db, _ = sql.Open("mysql", connect)
	return result
}

//Mysql 操作对象
type Mysql struct {
	db *sql.DB
}

//Close 关闭数据库连接
func (m *Mysql) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

//RowsCallbackResult 查询多条数据,结果以回调函数处理
//param sql string SQL
//param callback func(*sql.Rows) 回调函数指针
//param args... interface{} SQL参数
func (m *Mysql) RowsCallbackResult(sql string, callback gosql.RowsCallback, args ...interface{}) error {
	if err := m.connect(); err != nil {
		return err
	}
	rows, err := m.db.Query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if callback != nil {
		callback(rows)
	}
	return nil
}

func (m *Mysql) connect() error {
	if m.db == nil {
		return ErrorNotOpen
	}
	if err := m.db.Ping(); err != nil {
		return err
	}
	return nil
}

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
func (m *Mysql) Rows(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	if err := m.connect(); err != nil {
		return nil, err
	}
	rows, err := m.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []map[string]interface{}
	colums, _ := rows.Columns()
	for rows.Next() {
		var colmap = make(map[string]interface{}, 1)
		var refs = make([]interface{}, len(colums))
		for index, c := range colums {
			var ref interface{}
			colmap[c] = &ref
			refs[index] = &ref
		}
		err = rows.Scan(refs...)
		if err != nil {
			return nil, err
		}
		for k, v := range colmap {
			colmap[k] = *v.(*interface{})
		}
		result = append(result, colmap)
	}
	return result, nil
}

//Row 查询单条语句,返回结果
//param sql string SQL
//param args... interface{} SQL参数
func (m *Mysql) Row(sql string, args ...interface{}) (*sql.Row, error) {
	if err := m.connect(); err != nil {
		return nil, err
	}
	if ok, _ := regexp.MatchString("(?i)(.*?) LIMIT (.*?)\\s?(.*)?", sql); ok {
		sql = regexp.MustCompile("(?i)(.*?) LIMIT (.*?)\\s?(.*)?").ReplaceAllString(sql, "$1")
	} else {
		sql += " LIMIT 1 "
	}
	return m.db.QueryRow(sql, args...), nil
}

//Exec 执行一条SQL
//param sql string SQL
//param args... interface{} SQL参数
func (m *Mysql) Exec(sql string, args ...interface{}) (sql.Result, error) {
	if err := m.connect(); err != nil {
		return nil, err
	}
	return m.db.Exec(sql, args...)
}

//Count SQL语句条数统计
//param sql string SQL
//param args... interface{} SQL参数
func (m *Mysql) Count(sql string, args ...interface{}) (int64, error) {
	if ok, _ := regexp.MatchString("(?i)(.*?) LIMIT (.*?)\\s?(.*)?", sql); ok {
		sql = "SELECT COUNT(1) FROM (" + sql + ") as tmp"
	}
	if ok, _ := regexp.MatchString("(?i).* group by .*", sql); ok {
		sql = "SELECT COUNT(1) FROM (" + sql + ") as tmp"
	}
	sql = regexp.MustCompile("^(?i)select .*? from (.*) order by (.*)").ReplaceAllString(sql, "SELECT count(1) FROM $1")
	sql = regexp.MustCompile("^(?i)select .*? from (.*)").ReplaceAllString(sql, "SELECT count(1) FROM $1")
	if err := m.connect(); err != nil {
		return 0, err
	}
	result := m.db.QueryRow(sql, args...)
	var count int64
	err := result.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

//ParseSQL 解析SQL
//param sql string SQL
//param args map[string]interface{} 参数映射
func (m *Mysql) ParseSQL(sql string, args map[string]interface{}) (string, []interface{}, error) {
	cp, err := regexp.Compile("@([^\\s|,|\\)]*)")
	if err != nil {
		return sql, nil, nil
	}
	pts := cp.FindAllStringSubmatch(sql, -1)
	if pts != nil && args != nil { //匹配到数据
		result := make([]interface{}, len(pts))
		for index, s := range pts {
			if v, ok := args[s[1]]; ok { //存在参数
				result[index] = v
			} else {
				return sql, nil, errors.New("缺少参数[" + s[0] + "]的值")
			}
		}
		return cp.ReplaceAllString(sql, "?"), result, nil
	}
	return sql, nil, nil
}

//Transaction 事务处理
//param t TransactionFunc 事务处理函数
func (m *Mysql) Transaction(t gosql.TransactionFunc) error {
	if err := m.connect(); err != nil {
		return err
	}
	tx, err := m.db.Begin()
	if err == nil {
		if t != nil {
			err = t(tx)
			if err != nil {
				tx.Rollback()
			} else {
				err = tx.Commit()
				if err != nil { //事务提交失败,回滚事务,返回错误
					tx.Rollback()
				}
			}

		}
	}
	return err
}

//GetDb 获取数据库对象
func (m *Mysql) GetDb() (*sql.DB, error) {
	if err := m.connect(); err != nil {
		return nil, err
	}
	return m.db, nil
}
