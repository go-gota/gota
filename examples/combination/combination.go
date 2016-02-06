package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kniren/gota/data-frame"
)

func main() {
	d := df.DataFrame{}
	absPath, _ := filepath.Abs("../dataset.csv")
	csvfile, err := os.Open(absPath)
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

	// Original DataFrame
	fmt.Println("Original Dataframe:")
	fmt.Println(d)

	// Subsetting from the original dataframe
	da, err := d.SubsetRows(df.R{0, 3})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Subset A:")
	fmt.Println(da)
	db, err := d.SubsetRows(df.R{3, 4})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Subset B:")
	fmt.Println(db)

	// Let's combine both subsets
	fmt.Println("Row Combination A+B:")
	fmt.Println(df.Rbind(*da, *db))

	// Subsetting from the original dataframe
	dc, err := d.SubsetColumns([]string{})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Subset C:")
	fmt.Println(dc)
	dd, err := d.SubsetColumns([]string{"Date"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Subset D:")
	fmt.Println(dd)

	fmt.Println("Column Combination C+D:")
	fmt.Println(df.Cbind(*dc, *dd))
}
