package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/kniren/gota/data-frame"
)

// TODO: Write tests!
func main() {
	d := df.DataFrame{}
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

	err = d.LoadAndParse(records, map[string]string{"Volume": "float", "Age": "int", "Date": "date"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(d)
	fmt.Println(d.Columns["Age"])
	//fmt.Println(d.SubsetColumns([]string{"Date", "Country"}))
}
