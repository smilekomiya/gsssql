package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// Retrieve a token, saves the token, then returns the generated client.
func getSSClient(config *oauth2.Config) *http.Client {
	tokFile := "token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

type column struct {
	Field     string  `json:"Field"`
	Type      string  `json:"Type"`
	Collation *string `json:"Collation"`
	Null      string  `json:"Null"`
	Key       string  `json:"Key"`
	Default   *string `json:"Default"`
	Extra     string  `json:"Extra"`
	Comment   string  `json:"Comment"`
}

type indexStruct struct {
	NO         string `json:"NO"`
	NonUnique  string `json:"NonUnique"`
	IndexName  string `json:"IndexName"`
	ColumnName string `json:"ColumnName"`
}

func main() {
	ctx := context.Background()
	b, err := ioutil.ReadFile("credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	config, err := google.ConfigFromJSON(b, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}

	client := getSSClient(config)

	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	spreadsheetId := "your spreadsheet id"
	resp, err := srv.Spreadsheets.Get(spreadsheetId).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve data from sheet: %v", err)
	}

	sheetObjects := resp.Sheets

	// load table names from MySQL
	db, err := sql.Open("mysql", "root:password01@tcp(localhost:3308)/dbname")
	if err != nil {
		panic(err)
	}

	defer db.Close()

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		fmt.Println(err)
	}

	var table string
	var tables []string
	for rows.Next() {
		err := rows.Scan(&table)
		if err != nil {
			fmt.Println("error")
		}
		tables = append(tables, table)
	}

	reqs := []*sheets.Request{}
	for _, table := range tables {
		hasSheet := false
		for _, sheet := range sheetObjects {
			if sheet.Properties.Title == table {
				hasSheet = true
			}
		}

		if !hasSheet {
			gridProperties := &sheets.GridProperties{
				RowCount:    100,
				ColumnCount: 20,
			}
			reqs = append(reqs, &sheets.Request{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						Title:          table,
						GridProperties: gridProperties,
					},
				},
			})
		}
	}

	if len(reqs) > 0 {
		rbb := &sheets.BatchUpdateSpreadsheetRequest{
			Requests: reqs,
		}

		_, err := srv.Spreadsheets.BatchUpdate(spreadsheetId, rbb).Context(context.Background()).Do()
		if err != nil {
			log.Fatal(err)
		}
	}

	// データの挿入
	for _, table := range tables {
		for _, sheet := range sheetObjects {
			if sheet.Properties.Title == table {
				fmt.Printf("processing %s ...\n", table)
				_, err = srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
					Requests: []*sheets.Request{
						{
							RepeatCell: &sheets.RepeatCellRequest{
								Range: &sheets.GridRange{
									SheetId:          sheet.Properties.SheetId,
									StartRowIndex:    0,
									EndRowIndex:      1,
									StartColumnIndex: 0,
									EndColumnIndex:   8,
								},
								Cell: &sheets.CellData{
									UserEnteredFormat: &sheets.CellFormat{
										BackgroundColor: &sheets.Color{
											Red:   float64(65) / float64(255),
											Green: float64(105) / float64(255),
											Blue:  float64(225) / float64(255),
										},
										HorizontalAlignment: "CENTER",
										TextFormat: &sheets.TextFormat{
											Bold: false,
											ForegroundColor: &sheets.Color{
												Red:   float64(255) / float64(255),
												Green: float64(255) / float64(255),
												Blue:  float64(255) / float64(255),
											},
										},
									},
								},
								Fields: "*",
							},
						},
						{
							RepeatCell: &sheets.RepeatCellRequest{
								Range: &sheets.GridRange{
									SheetId:          sheet.Properties.SheetId,
									StartRowIndex:    0,
									EndRowIndex:      1,
									StartColumnIndex: 9,
									EndColumnIndex:   13,
								},
								Cell: &sheets.CellData{
									UserEnteredFormat: &sheets.CellFormat{
										BackgroundColor: &sheets.Color{
											Red:   float64(46) / float64(255),
											Green: float64(139) / float64(255),
											Blue:  float64(87) / float64(255),
										},
										HorizontalAlignment: "CENTER",
										TextFormat: &sheets.TextFormat{
											Bold: false,
											ForegroundColor: &sheets.Color{
												Red:   float64(255) / float64(255),
												Green: float64(255) / float64(255),
												Blue:  float64(255) / float64(255),
											},
										},
									},
								},
								Fields: "*",
							},
						},
					},
				}).Context(ctx).Do()

				if err != nil {
					log.Fatal(err)
				}

				// set column names
				columnSql := `select
							COLUMN_NAME AS 'Field',
							COLUMN_TYPE AS 'Type',
							COLLATION_NAME AS 'Collation',
							IS_NULLABLE AS 'Null',
							COLUMN_KEY AS 'Key',
							COLUMN_DEFAULT AS 'Default',
							EXTRA AS 'Extra',
							COLUMN_COMMENT AS 'Comment'
						from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME="%s" AND TABLE_SCHEMA="dbname"
						ORDER BY ORDINAL_POSITION;`

				columnRows, err := db.Query(fmt.Sprintf(columnSql, table))
				if err != nil {
					fmt.Println(err)
				}

				indexSql := `
							select
							SEQ_IN_INDEX AS 'NO',
							NON_UNIQUE AS 'NonUnique',
							INDEX_NAME AS 'IndexName',
							COLUMN_NAME AS 'ColumnName'
						from INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME="%s"  AND TABLE_SCHEMA="dbname";
						`

				indexRows, err := db.Query(fmt.Sprintf(indexSql, table))
				if err != nil {
					fmt.Println(err)
				}

				cellValues := []*sheets.ValueRange{{
					MajorDimension: "ROWS",
					Range:          fmt.Sprintf("%s!A1:M1", table),
					Values: [][]interface{}{
						{
							"カラム名",
							"型",
							"Collation",
							"Null",
							"Key",
							"Default",
							"Extra",
							"Comment",
							"",
							"インデックスNO",
							"NonUnique",
							"IndexName",
							"ColumnName",
						},
					},
				}}

				index := 2
				for columnRows.Next() {
					column := column{}
					columnRows.Scan(&column.Field, &column.Type, &column.Collation, &column.Null, &column.Key, &column.Default, &column.Extra, &column.Comment)

					columnCollation := ""
					if column.Collation == nil {
						columnCollation = "NULL"
					} else {
						columnCollation = *column.Collation
					}

					columnDefault := ""
					if column.Default == nil {
						columnDefault = "NULL"
					} else {
						columnDefault = *column.Default
					}

					cellValues = append(cellValues, &sheets.ValueRange{
						Range: fmt.Sprintf("%s!A%d:H%d", table, index, index),
						Values: [][]interface{}{
							{
								column.Field,
								column.Type,
								columnCollation,
								column.Null,
								column.Key,
								columnDefault,
								column.Extra,
								column.Comment,
							},
						},
					})
					index++
				}

				index = 2
				for indexRows.Next() {
					indexRow := indexStruct{}
					indexRows.Scan(&indexRow.NO, &indexRow.NonUnique, &indexRow.IndexName, &indexRow.ColumnName)

					cellValues = append(cellValues, &sheets.ValueRange{
						Range: fmt.Sprintf("%s!J%d:M%d", table, index, index),
						Values: [][]interface{}{
							{
								indexRow.NO,
								indexRow.NonUnique,
								indexRow.IndexName,
								indexRow.ColumnName,
							},
						},
					})
					index++
				}

				_, err = srv.Spreadsheets.Values.BatchUpdate(spreadsheetId, &sheets.BatchUpdateValuesRequest{
					ValueInputOption: "USER_ENTERED",
					Data:             cellValues,
				}).Context(ctx).Do()

				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("done")
				time.Sleep(2 * time.Second)
			}
		}
	}
}
