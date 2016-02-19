package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
    Info    *log.Logger
    Error   *log.Logger
)

func main() {
	_ = "breakpoint"
	initLogs(os.Stdout)
	props := loadProperties()
	csvfilename, nextmonth := downloadCSV(props)
	zipfilename := strings.Replace(csvfilename, "csv", "zip", -1)
	zipit(csvfilename, zipfilename)
	sendit(zipfilename, getProperty(props, "bucket"))
	deleteit(zipfilename, csvfilename)
	saveProperties(props)
}

func deleteit(zipfilename string, csvfilename string) {
	err := os.Remove(name string)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
}

func saveproperties(props map[string]interface{}) {

}

/**
 *   Run SQL statement and download to a CSV file
 **/
func downloadCSV(props map[string]interface{}) (l string, p string) {
	// Get properties
	connString := getProperty(props, "connectionString")
	nextmonth := getProperty(props, "nextmonth")
	supplierid := getProperty(props, "supplierid")
	filename := getProperty(props, "name")
	workingfolder := getProperty(props, "workingfolder")

	//get csv file Name
	t := time.Now()
	timestamp := t.Format("20060102150405")
	csvfilename := workingfolder + "/" + supplierid + "_" + filename + "_" + timestamp + ".csv"

	//create our temp folder
	os.Mkdir(workingfolder, 0777)

	// Open SQL connection
	db, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
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
		return nil, nil
	}
	defer csvfile.Close()
	writer := csv.NewWriter(csvfile)

	// Loop to write CSV file
	for rows.Next() {
		//err = rows.Scan(&rslt.SupplierID, &rslt.OrderDate, &rslt.Year, &rslt.Month, &rslt.Hour, &rslt.Quarter, &rslt.AccountID, &rslt.Name, &rslt.GroupCode, &rslt.GroupDecription, &rslt.RepCode, &rslt.RepName, &rslt.ProductID, &rslt.CategoryCode, &rslt.CategoryDescription, &rslt.Ordered, &rslt.Delivered, &rslt.LineTotal, &rslt.Cost)
		var row [19]string
		err = rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7], &row[8], &row[9], &row[10], &row[11], &row[12], &row[13], &row[14], &row[15], &row[15], &row[16], &row[17])
		if err != nil {
			log.Fatal("Scan failed:", err.Error())
		}
		err := writer.Write(row[:])
		if err != nil {
			fmt.Println("Error:", err)
			return nil, nil
		}
	}
	// Close CSV File
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatal(err)
	}
	return csvfilename, nextmonth
}

/*
 * Zip a file or folder
 */
func zipit(source, target string) error {
	zipfile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer zipfile.Close()

	archive := zip.NewWriter(zipfile)
	defer archive.Close()
	info, err := os.Stat(source)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(source)
	}

	filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		if baseDir != "" {
			header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, source))
		}
		if info.IsDir() {
			header.Name += "/"
		} else {
			header.Method = zip.Deflate
		}
		writer, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(writer, file)
		return err
	})
	return err
}

func sendit(fileName string, bucketName string) {
	client, err := google.DefaultClient(context.Background(), storage.DevstorageFullControlScope)
	if err != nil {
		log.Fatalf("Unable to get default client: %v", err)
	}
	service, err := storage.New(client)
	if err != nil {
		log.Fatalf("Unable to create storage service: %v", err)
	}
	object := &storage.Object{Name: fileName}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Error opening file", err.Error())
	}
	if res, err := service.Objects.Insert(bucketName, object).Media(file).Do(); err == nil {
		fmt.Printf("Created object %v at location %v\n\n", res.Name, res.SelfLink)
	} else {
		log.Fatal("Error sending ZIP file", err.Error())
	}
}

/**
 *   Shortcut to check for errors
 **/
func check(err error, msg string) {
	if err != nil {
		log.Fatal(msg, err.Error())
	}
}

/**
 *   Load properties from JSON
 **/
func loadProperties() map[string]interface{} {
	b, err := ioutil.ReadFile("producthistory.json")
	check(err, "Properties file no loaded")

	var data interface{}
	json.Unmarshal(b, &data)
	m := data.(map[string]interface{})
	return m
}

/**
 *   shortcut to get property by name
 **/
func getProperty(props map[string]interface{}, name string) string {
	value, ok := props[name].(string)
	if !ok {
		log.Fatal("No supplierID in properties")
	}
	return value
}

/**
 *   Get SQL from file
 **/
func loadSQL() string {
	b, err := ioutil.ReadFile("producthistory.sql")
	check(err, "SQL file no loaded")
	return string(b)
}

func initLogs(handler io.Writer) {
	Info = log.New(handler,"INFO: ",log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(handler,"ERROR: ",log.Ldate|log.Ltime|log.Lshortfile)
}
