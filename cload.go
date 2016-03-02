package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Rapidtrade/gotools/file"
	"github.com/Rapidtrade/gotools/gcloud"
)

// Variables for logging
var (
	Info  *log.Logger
	Error *log.Logger
)

const wfolder = "tmp"

func main() {
	Info, _ := file.InitLogs(os.Stdout)
	Info.Println("Starting cload ==>")
	os.Mkdir("tmp", 0777)

	// Download all zip files from cloud storage
	files, err := gcloud.DownloadGS("rapidtradeinbox", wfolder, true)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Unzip the rapidtardeinbox files one by one and upload to rapidtradepending bucket
	for _, zfile := range files {
		ext := filepath.Ext(zfile)
		if ext != ".zip" {
			continue
		}

		//Unzip and send to rapidtradePending bucket
		ufiles, _ := file.UnZipIT(wfolder, zfile)
		for _, ufile := range ufiles {
			//send CSV back to GS folder so we can load it via a job
			err = gcloud.SendGS("rapidtradepending", "", filepath.Join(wfolder, ufile))
			if err != nil {
				Error.Println(err)
			}
		}
	}

	// Send pending files to BigQuery
	pfiles, err := gcloud.ListBucketGS("rapidtradepending")
	if err != nil {
		Error.Fatal(err)
	}

	//loop through csv files and start jobs to load to bigquery
	for _, pfile := range pfiles {
		Info.Println("Processing " + pfile)
		// Get table names & create BigQuery table
		supplierID := pfile[0:strings.Index(pfile, "_")]
		tableName := pfile[0:strings.LastIndex(pfile, "_")]
		schemaName := pfile[strings.Index(pfile, "_")+1:strings.LastIndex(pfile, "_")] + "Schema.json"

		// Create table if needed. Will get error if table exists already, but thats fine
		err = gcloud.CreateTableBQ(schemaName, "citric-optics-107909", "History", tableName)
		if err == nil {
			Info.Println(tableName + " created sucessfully.")
		}
		gsURI := "gs://rapidtradepending/" + pfile

		//Create the job
		jobId, err := gcloud.InsertJobBQ("jobConfig.json", "citric-optics-107909", "History", tableName, gsURI)
		if err != nil {
			Error.Println("Job not succesfull for " + gsURI)
			continue
		}
		Info.Println("Job " + jobId + " created for " + gsURI)

		// Wait for the job to finish sleeping 10 seconds at a time, only wait for 100 seconds for job to complete
		x := 1
		for {
			time.Sleep(10 * time.Second)
			if gcloud.JobStatusBQ("citric-optics-107909", jobId) {
				break
			}
			x++
			if x > 10 {
				Info.Println("Job not finished, continue with other files")
				continue
			}
		}
		//  Now move file file from the pending to archive
		err = gcloud.CopyGS("rapidtradepending", pfile, "rapidtradearchive", supplierID+"/"+pfile)
		if err != nil {
			Error.Println(err)
			continue
		}

		// Finaly delete the file in pending
		err = gcloud.DeleteGS("rapidtradepending", pfile)
		if err != nil {
			Error.Println(err)
			continue
		}
	}
	Info.Println("Finished cload")
}

/*
//Below is reference for the job file needed
{
  "jobReference":
  {
      "projectId": "citric-optics-107909",
      "jobId": "12345678901"
  },
  "configuration":
  {
    "load":
    {
      "sourceUris":
      [
      ],
      "destinationTable":
      {
        "datasetId": "History",
        "projectId": "citric-optics-107909",
        "tableId": "LILGREEN_ProductHistory"
      },
      "schema":
      {
        "fields":
        [
          {
            "name": "SupplierID",
            "type": "STRING"
          },
          {
            "name": "OrderDate",
            "type": "STRING"
          },
          {
            "name": "Year",
            "type": "INTEGER"
          },
          {
            "name": "Month",
            "type": "INTEGER"
          },
          {
            "name": "Hour",
            "type": "INTEGER"
          },
          {
            "name": "Quarter",
            "type": "INTEGER"
          },
          {
            "name": "AccountID",
            "type": "STRING"
          },
          {
            "name": "Name",
            "type": "STRING"
          },
          {
            "name": "GroupCode",
            "type": "STRING"
          },
          {
            "name": "GroupDescription",
            "type": "STRING"
          },
          {
            "name": "RepCode",
            "type": "STRING"
          },
          {
            "name": "RepName",
            "type": "STRING"
          },
          {
            "name": "ProductID",
            "type": "STRING"
          },
          {
            "name": "CategoryCode",
            "type": "STRING"
          },
          {
            "name": "CategoryDescription",
            "type": "STRING"
          },
          {
            "name": "Ordered",
            "type": "FLOAT"
          },
          {
            "name": "Delivered",
            "type": "FLOAT"
          },
          {
            "name": "LineTotal",
            "type": "FLOAT"
          },
          {
            "name": "Cost",
            "type": "FLOAT"
          }
        ]
      }

    }
  }

}

*/
