package main

import (
	"encoding/csv"
	"fmt"
	. "kniren/gota/data-frame"
	"os"
)

// TODO: Write tests!
func main() {
	// Test 02
	//textColumn := []string{"One", "Two", "Three"}
	//intColumn := []int{1, 2, 3}
	//c1 := Column{}
	//c1.fillColumn(textColumn)
	//c2 := Column{}
	//c2.fillColumn(intColumn)
	//df := DataFrame{
	//columns:  []Column{c1, c2},
	//nCols:    2,
	//nRows:    3,
	//colnames: []string{"Text", "Ints"},
	//}
	//fmt.Println(df)

	// Test 01
	//in := `A,B,C,D
	//1,2,3,4
	//5,6,7,8`
	//df := DataFrame{}
	//r := csv.NewReader(strings.NewReader(in))
	//records, err := r.ReadAll()
	//if err != nil {
	//panic(err)
	//}
	//err = df.loadAndParse(records, []string{"string", "int", "string", "int"})
	//if err != nil {
	//panic(err)
	//}

	//for _, v := range df.columns {
	//fmt.Println(v)
	//}
	//fmt.Println(df)

	// Test 03
	df := DataFrame{}
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
	//df.loadData(records)
	//err = df.loadAndParse(records, []string{"string", "int", "string", "int", "int", "int", "int", "int", "int", "int"})
	err = df.LoadAndParse(records, map[string]string{"BidiCode": "int"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(df)
	//fmt.Println(df.SubsetColumns([]string{"Biobank", "Patient", "BoxId"}))
}
