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
	fmt.Println(d)
	d.Unique()
}
