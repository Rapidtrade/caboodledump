package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Rapidtrade/gotools/file"
	"github.com/Rapidtrade/gotools/gcloud"

	_ "github.com/denisenkom/go-mssqldb"
)

// Variables for logging
var (
	Info  *log.Logger
	Error *log.Logger
)

func main() {
	// Initialise logs
	Info, Error := file.InitLogs(os.Stdout)
	Info.Println("Starting cdump =================>")

	// Load properties
	props, err := file.LoadProperties("producthistory.json")
	if err != nil {
		Error.Println(err)
		log.Fatal(err)
	}

	// Download SQL query, zip & send to GS
	csvfilename, nextmonth := downloadCSV(props)
	//current := iscurrent(nextmonth)
	zipfilename := strings.Replace(csvfilename, "csv", "zip", -1)
	file.ZipIT(csvfilename, zipfilename)
	Info.Println("Sending: " + zipfilename)
	_ = "breakpoint"
	err = gcloud.SendGS(file.GetProperty(props, "bucket"), "", zipfilename)
	if err != nil {
		Error.Println(err)
	} else {
		savelastmonth(props, nextmonth)
	}
}

// Returns true we busy reading current month
func iscurrent(nextmonth string) bool {
	next, _ := time.Parse("060102", nextmonth+"01")
	var dy int
	dy = time.Now().Day() * -1
	firstDOM := time.Now().AddDate(0, 0, dy)
	return next.Equal(firstDOM)
}

func savelastmonth(props map[string]interface{}, nextmonth string) {
	next, _ := time.Parse("060102", nextmonth+"01")
	next = next.AddDate(0, 1, 0)
	nextMonth := next.Format("0601")
	props["nextmonth"] = nextMonth

	b, err := json.Marshal(props)
	if err != nil {
		log.Fatal(err)
	}

	err = ioutil.WriteFile("producthistory.json", b, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

/**
 *   Run SQL statement and download to a CSV file
 **/
func downloadCSV(props map[string]interface{}) (csvfilename string, nextmonth string) {
	// Get properties
	connString := file.GetProperty(props, "connectionString")
	nextmonth = file.GetProperty(props, "nextmonth")
	supplierid := file.GetProperty(props, "supplierid")
	filename := file.GetProperty(props, "name")
	workingfolder := file.GetProperty(props, "workingfolder")

	//get csv file Name
	csvfilename = workingfolder + "/" + supplierid + "_" + filename + "_" + nextmonth + ".csv"

	//create our temp folder
	os.Mkdir(workingfolder, 0777)

	// Open SQL connection
	db, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
		return "", ""
	}
	defer db.Close()

	//Get SQL & replace <lastyear> with last date
	sql := loadSQL()
	sql = strings.Replace(sql, "<nextmonth>", nextmonth, -1)

	type ProductHistory struct {
		SupplierID          string
		OrderDate           string
		Year                int32
		Month               int32
		Hour                int32
		Quarter             int32
		AccountID           string
		Name                string
		GroupCode           string
		GroupDecription     string
		RepCode             string
		RepName             string
		ProductID           string
		CategoryCode        string
		CategoryDescription string
		Ordered             int32
		Delivered           int32
		LineTotal           float32
		Cost                float32
	}

	//Get rows and load into structure
	rows, err := db.Query(sql)
	defer rows.Close()

	//open csv filename
	csvfile, err := os.Create(csvfilename)
	if err != nil {
		fmt.Println("Error:", err)
		return "", ""
	}
	defer csvfile.Close()
	writer := csv.NewWriter(csvfile)

	// Loop to write CSV file
	for rows.Next() {
		//err = rows.Scan(&rslt.SupplierID, &rslt.OrderDate, &rslt.Year, &rslt.Month, &rslt.Hour, &rslt.Quarter, &rslt.AccountID, &rslt.Name, &rslt.GroupCode, &rslt.GroupDecription, &rslt.RepCode, &rslt.RepName, &rslt.ProductID, &rslt.CategoryCode, &rslt.CategoryDescription, &rslt.Ordered, &rslt.Delivered, &rslt.LineTotal, &rslt.Cost)
		var row [21]string
		err = rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7], &row[8], &row[9], &row[10], &row[11], &row[12], &row[13], &row[14], &row[15], &row[15], &row[16], &row[17], &row[18], &row[19])
		if err != nil {
			log.Fatal("Scan failed:", err.Error())
		}
		err := writer.Write(row[:])
		if err != nil {
			fmt.Println("Error:", err)
			return "", ""
		}
	}
	// Close CSV File
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatal(err)
	}
	return csvfilename, nextmonth
}

/**
 *   Get SQL from file
 **/
func loadSQL() string {
	b, err := ioutil.ReadFile("producthistory.sql")
	if err != nil {
		log.Fatal(err.Error())
	}
	return string(b)
}
