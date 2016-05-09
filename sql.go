package gosql

import (
	"ShopSupplierApi/utils"
	"database/sql"
	"errors"
	"regexp"
	"strconv"
)

//db 数据库对象
var db *sql.DB

//RowsCallback Rows数据结果回调函数
type RowsCallback func(rows *sql.Rows)

//TransactionFunc 事务回调函数
type TransactionFunc func(tx *sql.Tx) error

//ErrorNotOpen 数据库开启错误
var ErrorNotOpen = errors.New("Database not open,please call Open function before")

//Open 打开数据库连接,注意在使用任何数据库操作之前必须先调用该方法.
//该方法只需调用一次
//param username string 用户名
//param password string 密码
//param database string 数据库名称
//param params[0]-ulr string 数据库连接地址
//param params[1]-port int 数据库端口
func Open(username string, password string, database string, params ...interface{}) error {
	var port int
	var url string
	if len(params) > 1 {
		url = params[0].(string)
		port = params[1].(int)
	} else if len(params) > 0 {
		url = params[0].(string)
		port = 3306
	} else {
		url = "127.0.0.1"
		port = 3306
	}
	var err error
	Db, err = sql.Open("mysql", username+":"+password+"@tcp("+url+":"+strconv.Itoa(port)+")/"+database+"?loc=Local")
	if err != nil {
		utils.Logger.Error(err.Error())
	}
	return err
}

//Close 关闭数据库连接
func Close() {
	if Db != nil {
		Db.Close()
	}
}

//RowsCallbackResult 查询多条数据,结果以回调函数处理
//param sql string SQL
//param callback func(*sql.Rows) 回调函数指针
//param args... interface{} SQL参数
func RowsCallbackResult(sql string, callback RowsCallback, args ...interface{}) error {
	if Db == nil {
		return ErrorNotOpen
	}
	if err := Db.Ping(); err != nil {
		return err
	}
	utils.Logger.Debug(sql+" \n\tArgs:", args...)
	rows, err := Db.Query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if callback != nil {
		callback(rows)
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
func Rows(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	if Db == nil {
		return nil, ErrorNotOpen
	}
	if err := Db.Ping(); err != nil {
		return nil, err
	}
	utils.Logger.Debug(sql+" \n\tArgs:", args...)
	rows, err := Db.Query(sql, args...)
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
func Row(sql string, args ...interface{}) (*sql.Row, error) {
	if Db == nil {
		return nil, errors.New("Database not open,please call Open function before")
	}
	if err := Db.Ping(); err != nil {
		return nil, err
	}
	if ok, _ := regexp.MatchString("(?i)(.*?) LIMIT (.*?)\\s?(.*)?", sql); ok {
		sql = regexp.MustCompile("(?i)(.*?) LIMIT (.*?)\\s?(.*)?").ReplaceAllString(sql, "$1")
	} else {
		sql += " LIMIT 1 "
	}
	utils.Logger.Debug(sql+" \n\tArgs:", args...)
	return Db.QueryRow(sql, args...), nil
}

//Exec 执行一条SQL
//param sql string SQL
//param args... interface{} SQL参数
func Exec(sql string, args ...interface{}) (sql.Result, error) {
	if Db == nil {
		return nil, errors.New("Database not open,please call Open function before")
	}
	if err := Db.Ping(); err != nil {
		return nil, err
	}
	utils.Logger.Debug(sql+" \n\tArgs:", args...)
	return Db.Exec(sql, args...)
}

//Count SQL语句条数统计
//param sql string SQL
//param args... interface{} SQL参数
func Count(sql string, args ...interface{}) (int64, error) {
	if ok, _ := regexp.MatchString("(?i)(.*?) LIMIT (.*?)\\s?(.*)?", sql); ok {
		sql = "SELECT COUNT(1) FROM (" + sql + ") as tmp"
	}
	if ok, _ := regexp.MatchString("(?i).* group by .*", sql); ok {
		sql = "SELECT COUNT(1) FROM (" + sql + ") as tmp"
	}
	sql = regexp.MustCompile("^(?i)select .*? from (.*) order by (.*)").ReplaceAllString(sql, "SELECT count(1) FROM $1")
	sql = regexp.MustCompile("^(?i)select .*? from (.*)").ReplaceAllString(sql, "SELECT count(1) FROM $1")
	if Db == nil {
		return 0, errors.New("Database not open,please call Open function before")
	}
	var err error
	if err = Db.Ping(); err != nil {
		return 0, err
	}
	utils.Logger.Debug(sql+" \n\tArgs:", args...)
	result := Db.QueryRow(sql, args...)
	var count int64
	err = result.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

//ParseSQL 解析SQL
//param sql string SQL
//param args map[string]interface{} 参数映射
func ParseSQL(sql string, args map[string]interface{}) (string, []interface{}, error) {
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
func Transaction(t TransactionFunc) error {
	if Db == nil {
		return errors.New("Database not open,please call Open function before")
	}
	var err error
	if err = Db.Ping(); err != nil {
		return err
	}
	tx, err := Db.Begin()
	if err == nil {
		if t != nil {
			err = t(tx)
			if err != nil {
				tx.Rollback()
			} else {
				err = tx.Commit()
				if err != nil { //事务提交失败,回滚事务,返回错误
					utils.Logger.Error("事务提交失败 ", err.Error())
					tx.Rollback()
				}
			}

		}
	} else {
		utils.Logger.Error("事务开启失败 ", err.Error())
	}
	return err
}
