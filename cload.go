package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/Rapidtrade/gotools/file"
	"github.com/Rapidtrade/gotools/gcloud"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/storage/v1"
)

// Variables for logging
var (
	Info  *log.Logger
	Error *log.Logger
)

const wfolder = "tmp"

func main() {
	_ = "breakpoint"
	Info, _ := file.InitLogs(os.Stdout)
	Info.Println("Starting cload ==>")
	os.Mkdir("tmp", 0777)

	// Download all zip files from cloud storage
	files, err := file.DownloadGS("rapidtradeinbox", wfolder)
	if err != nil {
		log.Fatal(err)
	}

	// Unzip the files one by one
	for _, zfile := range files {
		ext := filepath.Ext(zfile)
		if ext != ".zip" {
			continue
		}
		//Get SupplierID
		tableName := zfile[0:strings.LastIndex(zfile, "_")]
		schemaName := zfile[strings.Index(zfile, "_")+1:strings.LastIndex(zfile, "_")] + "Schema.json"

		gcloud.CreateTable(schemaName, "citric-optics-107909", "History", tableName)

		ufiles, _ := file.UnZipIT(wfolder, zfile)
		for _, ufile := range ufiles {
			err = readCSV(wfolder, ufile)
			if err != nil {
				Error.Println(err)
			}
		}
	}
}

/*
func createTable() error {
	client, err := google.DefaultClient(context.Background(), storage.DevstorageFullControlScope)
	if err != nil {
		return err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		log.Fatal(err)
	}
	tablesService := bigquery.NewTablesService(bq)


		fields := make([]*bigquery.TableFieldSchema, 0)
		var f1 bigquery.TableFieldSchema
		f1.Name = "SupplierID"
		f1.Type = "String"
		fields = append(fields, f1)

		var schema *bigquery.TableSchema
		schema.Fields = fields

		table := &bigquery.Table{
			Schema: schema,
			TableReference: &bigquery.TableReference{
				ProjectId: "A",
				DatasetId: "B",
				TableId:   "C",
			},
		}

	table := &bigquery.Table{}
	f, err := ioutil.ReadFile("producthistoryschema.json")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	if err = json.Unmarshal(f, &table); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}

	_, err = tablesService.Insert("citric-optics-107909", "History", table).Do()
	return err
}
*/

func readCSV(folder string, filename string) error {

	client, err := google.DefaultClient(context.Background(), storage.DevstorageFullControlScope)
	if err != nil {
		return err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		log.Fatal(err)
	}

	csvfile, err := os.Open(filepath.Join(folder, filename))
	if err != nil {
		return err
	}
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 0)
	reader := csv.NewReader(csvfile)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		// create a new row MAP
		row := &bigquery.TableDataInsertAllRequestRows{
			Json: make(map[string]bigquery.JsonValue, 0),
		}
		row.Json["SupplierID"] = record[0]
		row.Json["OrderDate"] = record[1]
		row.Json["Year"], _ = strconv.Atoi(record[2])
		row.Json["Month"], _ = strconv.Atoi(record[3])
		row.Json["Hour"], _ = strconv.Atoi(record[4])
		row.Json["Quarter"], _ = strconv.Atoi(record[5])
		row.Json["AccountID"] = record[6]
		row.Json["Name"] = record[7]
		row.Json["GroupCode"] = record[8]
		row.Json["GroupDescription"] = record[9]
		row.Json["RepCode"] = record[10]
		row.Json["RepName"] = record[11]
		row.Json["ProductID"] = record[12]
		row.Json["CategoryCode"] = record[13]
		row.Json["CategoryDescription"] = record[14]
		row.Json["Ordered"], _ = strconv.Atoi(record[15])
		row.Json["Delivered"], _ = strconv.ParseFloat(record[16], 64)
		row.Json["LineTotal"], _ = strconv.ParseFloat(record[17], 64)
		row.Json["Cost"], _ = strconv.ParseFloat(record[18], 64)

		rows = append(rows, row)

	}
	_ = "breakpoint"
	//Create a new map to hold the rows of data
	req := &bigquery.TableDataInsertAllRequest{
		Rows: rows,
	}
	call := bq.Tabledata.InsertAll("citric-optics-107909", "History", "LILGREEN_ProductHistory", req)
	resp, err := call.Do()
	if err != nil {
		return err
	}

	buf, _ := json.Marshal(resp)
	log.Print(string(buf))
	return nil
}
