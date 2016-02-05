package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/kniren/gota/data-frame"
)

func main() {
	d := df.DataFrame{}
	csvfile, err := os.Open("dataset.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	r := csv.NewReader(csvfile)
	records, err := r.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = d.LoadAndParse(records, df.T{"Age": "int", "Date": "date", "Amount": "float64"})
	if err != nil {
		fmt.Println(err)
		return
	}

	// Print df.Column to console
	fmt.Println(d.Columns["Age"])
	fmt.Println(d.Columns["Country"])
	fmt.Println(d.Columns["Date"])
	fmt.Println(d.Columns["Amount"])

	// Print a DataFrame to console
	fmt.Println(d)

	// Subset by column and rearrange the columns by name on the given order
	dd, _ := d.SubsetColumns([]string{"Date", "Country"})
	fmt.Println(dd)

	// Subset by column using a range element
	dd, _ = d.SubsetColumns(df.R{0, 1})
	fmt.Println(dd)

	// Subset by column using an array of column numbers
	dd, _ = d.SubsetColumns([]int{0, 3, 1})
	fmt.Println(dd)

	// Subset by rows using a range element
	dd, _ = d.SubsetRows(df.R{0, 1})
	fmt.Println(dd)

	// Subset by column using an array of row numbers
	dd, _ = d.SubsetRows([]int{0, 2, 1})
	fmt.Println(dd)

	// Subset by both columns and rows any subsetting format can be used
	dd, _ = d.Subset([]string{"Date", "Age"}, df.R{0, 2})
	fmt.Println(dd)
}
