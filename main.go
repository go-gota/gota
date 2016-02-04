package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/kniren/gota/data-frame"
)

// TODO: Write tests!
func main() {
	df := df.DataFrame{}
	csvfile, err := os.Open("example.csv")
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

	err = df.LoadAndParse(records, map[string]string{"Volume": "int", "Age": "int", "Date": "date"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(df)
	fmt.Println(df.SubsetColumns([]string{"Date", "Country"}))
}
