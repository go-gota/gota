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
  `Int`, `Float`, & `String`)
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
### Types
Each column in a DataFrame can only have elements of a given type. To
be able parse columns to different types, sort them and keep
everything compatible the types included into a column have to comply
with the `cell` interface. Right now the `cell` interface have the
following methods:
```
type cell interface {
	String() string
	Int() (*int, error)
	Float() (*float64, error)
	Bool() (*bool, error)
	NA() bool
	Checksum() [16]byte
}
```

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
err = d.LoadAndParse(records, df.T{"Age": "int", "Amount": "float"})
if err != nil {
    fmt.Println(err)
    return
}

// Create a new DataFrame with a custom constructor
d, err := df.New(
    df.C{"A", df.Strings("a", "b", "c")},
    df.C{"B", df.Ints(1, nil, 2)},
    df.C{"C", df.Ints(1, 2, 3)},
)
```

### Print to console
```
// Print a DataFrame to console
fmt.Println(d)

>      Country         Date        Age  Amount  Id
>
>   0: United States   2012-02-01  50   112.1   01234
>   1: United States   2012-02-01  32   321.31  54320
>   2: United Kingdom  2012-02-01  17   18.2    12345
>   3: United States   2012-02-01  32   321.31  54320
>   4: United Kingdom  2012-02-01  NA   18.2    12345
>   5: United States   2012-02-01  32   321.31  54320
>   6: United States   2012-02-01  32   321.31  54320
>   7: Spain           2012-02-01  66   555.42  00241

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
