Gota: DataFrames and Data Wrangling for Go
==========================================

This is an initial implementation of DataFrames for the Go programming
language (Golang). This is still at a very early stage of development
and changes to the API are to be expected. *Use at your own risk*.

What is a DataFrame
-------------------
The term DataFrame typically refers to a tabular dataset that can be
viewed as a two dimensional table. Often the columns of this dataset
refers to a list of features, while the rows represent a number of
measurements.

Common examples of DataFrames can be found on Excel sheets, CSV files
or data originated on a SQL database, but this data can come on
a variety of other formats, like a collection of JSON objects or XML
files.

The utility of DataFrames resides on the ability to subset them, merge
them, summarize the data for individual features or apply functions to
entire rows or columns.

When used in data analysis, techniques like Split Apply Combine can be
used to obtain insightful information for our dataset.

Motivation
----------
Golang powerful features for backend and API servers make it very
appealing to a lot of programmers, but it is severly lacking when
trying to use it for data science.

Sure, there are fantastic packages out there that allow us to perform
data analysis over numeric elements, like [gonum][1]. But what if what
we really want is to retrieve data from some source, like a file or
a database? What if we have different types of data on said dataset?
What if we want to subset the data using those non-numeric features?
What if after processing this data we want to pass it nicely formatted
to our javascript frontend via json for visualization?

We could always resort to call some dedicated statistical software
from Go to perform this functions and use the excellent marshaling
capabilities to transform it to our desired format, but I think it
would be much nicer and less cumbersome to be able to perform all
these actions from within Go.

Since I couldn't find any packages out there that fulfil my
requirements I decided to create it myself.The ambition is to create
a package that:

- [x] Load/save CSV data
- [ ] Load/save XML data
- [ ] Load/save JSON data
- [x] Parse loaded data to the given types (Currently supported:
  `int`, `float64`, `date` & `string`)
- [x] Row/Column subsetting (Indexing, column names, row numbers, range)
- [x] Unique/Duplicate row subsetting
- [ ] Conditional subsetting (i.e.:`Age > 35 && City == "London"`)
- [x] DataFrame combinations by rows and columns (cbind/rbind)
- [ ] DataFrame merging by keys (Inner, Outer, Left, Right, Cross)
- [ ] Function application over rows
- [ ] Function application over columns
- [ ] Statistics and summaries over the different features (Type dependant)
- [ ] Value counting (For histogram representations)
- [ ] Conversion between wide and long formats

Usage
-----

### Loading data
```
d := df.DataFrame{}
absPath, _ := filepath.Abs("dataset.csv")
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

// Load the data as string columns
err = d.LoadData(records)
if err != nil {
    fmt.Println(err)
    return
}

// Load and parse the features to the given format
err = d.LoadAndParse(records, df.T{"Age": "int", "Date": "date", "Amount": "float64"})
if err != nil {
    fmt.Println(err)
    return
}
```

### Print to console
```
// Print df.Column to console
fmt.Println(d.Columns["Age"])
fmt.Println(d.Columns["Country"])
fmt.Println(d.Columns["Date"])
fmt.Println(d.Columns["Amount"])

// Print a DataFrame to console
fmt.Println(d)
```
    
### Subsetting
```
// Subset by column and rearrange the columns by name on the given order
d1, err := d.SubsetColumns([]string{"Date", "Country"})

// Subset by column using a range element
d2, err := d.SubsetColumns(df.R{0, 1})

// Subset by column using an array of column numbers
d3, err := d.SubsetColumns([]int{0, 3, 1})

// Subset by rows using a range element
d4, err := d.SubsetRows(df.R{0, 1})

// Subset by column using an array of row numbers
d5, err := d.SubsetRows([]int{0, 2, 1})

// Subset by both columns and rows any subsetting format can be used
d6, err := d.Subset([]string{"Date", "Age"}, df.R{0, 2})

// Only unique elements
d7, err := d.Unique()

// Only duplicated elements
d8, err := d.Duplicated()
```

### Column/Row combinations
```
da, _ := d.SubsetRows(df.R{0, 3})
db, _ := d.SubsetRows(df.R{3, 4})
dc, _ := d.SubsetColumns([]string{"Age", "Country"})
dd, _ := d.SubsetColumns([]string{"Date"})

fmt.Println(df.Rbind(*da, *db))
fmt.Println(df.Cbind(*dc, *dd))
```

[1]: https://github.com/gonum
[2]: https://github.com/kniren/gota
