package main

import (
    "bufio"
		"database/sql"
    "fmt"
    "log"
    "os"
		"strings"

		_ "github.com/go-sql-driver/mysql"

	// config file package
	beegoConfig "github.com/astaxie/beego/config"
)

// vars - database
var db *sql.DB
var databaseName = ""
var tableName = ""

// vars - csv
var headerRow []string

var config beegoConfig.ConfigContainer

func main() {
    file, err := os.Open("./test.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

		// load configs
		config, err = beegoConfig.NewConfig("ini", "secrets.config")
		if err != nil {
			fmt.Println(err)
		}

		conxString := config.String("mysql::username") +
			":"  + config.String("mysql::password") +
			"@(" + config.String("mysql::host") +
			":"  + config.String("mysql::port") +
			")/" + config.String("mysql::dbname") + "?parseTime=true"

		db, err = sql.Open("mysql", conxString)
		if err != nil {
			fmt.Println(err)
		}
		defer db.Close()

		// db configs
		databaseName = config.String("mysql::databaseName")
		tableName = config.String("mysql::tableName")

		// set sql configs
		linesPerSqlOperation, _ := config.Int("mysql::linesPerSqlOperation")
		headersAtLineNumber, _ := config.Int("mysql::headersAtLineNumber")

		lineNumber := -1

		var lines []string

		// file is an io.Reader
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
				// start of loop logic
			  lineNumber = lineNumber + 1

				// header logic
				if (lineNumber == headersAtLineNumber) {

					// set the headers
					r := scanner.Text()
					r = strings.Replace(r, `"`, ``, -1)
					headerRow = strings.Split(r, ",")

					continue
				}

				// skip line logic
				if (lineNumber <= headersAtLineNumber) {
					continue
				}

				// ingest the line

				lines = append(lines, scanner.Text())

				if (len(lines) > linesPerSqlOperation) {
					storeLines(lines)
					lines = nil
				}

				// end of loop logic
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
}

func storeLines(lines []string) {
	sql := generateSql(lines)
	fmt.Println(sql)

	_, err := db.Exec(sql)

	if err != nil {
		fmt.Println(err)
	}
}

func generateSql(lines []string) string {

	// INSERT INTO database.tablename (field1, field2)
	// VALUES (field1_value1, field2_value1), (field1_value2, field2_value2)
	// ON DUPLICATE KEY
	// UPDATE field1 = VALUES(`field1`)

	sql := ``
	sql += `INSERT INTO` + ` ` + databaseName + `.` + tableName + ` ` + fieldsToSql() + ` `
	sql += `VALUES` + ` ` + linesToValueSql(lines) + ` `
	sql += `ON DUPLICATE KEY` + ` `
	sql += `UPDATE` + ` ` + fieldsToUpdateSql()

	return sql
}

func fieldsToSql() string {
	result := ``

	result += "("
	for i , e := range headerRow {
		if (i != 0 ) {
			result += ","
		}
		result += e
	}
	result += ")"

	return result
}

func linesToValueSql(lines []string) string {
	result := ""

	for i, line := range lines {
		if i != 0 {
			result += ","
		}

		cells := strings.Split(line, ",")

		result += "("
		for ii, cell := range cells {
			if ii != 0 {
				result += ","
			}
			result += cell
		}
		result += ")"
	}

	return result
}

func fieldsToUpdateSql() string {
	result := ``

	for i, e := range headerRow {
		if i != 0 {
			result += ","
		}
		result += e + " = VALUES(`" + e + "`)"
	}

	return result
}
