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

Usage
-----
### Series
On version `0.5.0` it was introduced the concept of Series. Series are
essentially vectors of elements. Within a Series there can only exist
elements of a single type and there is support for missing elements,
since in the real world we are going to have to handle less than
perfect data.

Four types are supported right now:

- Int
- Float
- String
- Bool

Series are the building blocks of DataFrames and they can be compared
between each other and against other types. They can be sliced as well
as expanded. Series objects have also a Name field that will be used
as the column name if it gets inserted into a DataFrame

There are four different constructors for Series objects, one for
every one of the supported types and they come in two flavors, named
and not named.

```
Strings()
Ints()
Floats()
Bools()
NamedStrings()
NamedInts()
NamedFloats()
NamedBools()
```

These constructors are fairly powerful and they accept a number of
different inputs, for example, you might want to create a String
Series from numbers:

```
a := Strings(1,2,3,4)
a = Strings([]int{1,2,3,4})
a = Strings([]float64{1,2,3,4})
a = Strings([]float64{1,2}, []int{3,4}, 5, 6)
```

You can even create a Series from another Series:

```
b := Floats(Ints(1,2,3,4), Floats(5,4,3,2,1))
```

Note that you might have NA values if you try to convert the wrong
thing!

```
// The first element should work fine, but the last two won't be
// parsed correctly
c := Ints([]float64{1.0, 3.4, 3.6})
```

### Loading data
```
// You can load data from different formats, for example, csv
s := `
Country,Date,Age,Amount,Id
"United States",2012-02-01,50,112.1,01234
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,17,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,NA,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United States",2012-02-01,32,321.31,54320
Spain,2012-02-01,66,555.42,00241
`
// Load from a CSV string, the types of the columns will be inferred
a := df.ReadCSV(s)

// You can do the same from [][]string...
r := csv.NewReader(strings.NewReader(s))
records, err := r.ReadAll()
if err != nil {
    log.Fatal(err)
}
a = df.ReadRecords(records)

// ... And also from JSON and other types
// a := df.ReadJSON(io.Reader)
// a := df.ReadJSONString(string)
// a := df.ReadMaps([]map[string]interface{})

// You can also specify the types of the columns to avoid confusion (In our
// example, look at the Id column, it clearly has some zeros to the left, but
// if the type is inferred as Int or Float we will lose them!).
// If in addition to the given argument you pass one string containing one of
// the following valid types, all columns will be parsed as that type. If
// N number of types is given, where N is the number of columns, each column
// will be parsed accordingly. Otherwise, we will get an error.
// Valid types:
//   "string"
//   "int"
//   "float"
//   "bool"
a = df.ReadRecords(records, "string") // All columns parsed as String Series
a = df.ReadRecords(
    records,
    "string",
    "string",
    "int",
    "float",
    "string",
) // Columns will be parsed accordingly

// Analogously, you can save DataFrames to CSV, JSON, and other formats
b := a.SaveRecords()
//b := a.SaveMaps()
//b, err := a.SaveJSON()
//b, err := a.SaveCSV()

// If you want you can create a DataFrame from a set of Series
c := df.New(
    df.Ints(1, 2, 3, 4),
    df.Strings("a", "b", "c", "d"),
)

// If the Series have names, these will be used as column names
c = df.New(
    df.NamedInts("YAY!", 1, 2, 3, 4),
    df.Strings("a", "b", "c", "d"),
)
```

### Print to console
```
// Print a DataFrame to console
fmt.Println(a)

>      Country         Date        Age  Amount  Id
>   0: United States   2012-02-01  50   112.1   01234
>   1: United States   2012-02-01  32   321.31  54320
>   2: United Kingdom  2012-02-01  17   18.2    12345
>   3: United States   2012-02-01  32   321.31  54320
>   4: United Kingdom  2012-02-01  NA   18.2    12345
>   5: United States   2012-02-01  32   321.31  54320
>   6: United States   2012-02-01  32   321.31  54320
>   7: Spain           2012-02-01  66   555.42  00241

```
    
### Subsetting, Filtering, etc.
DataFrames support a number of methods for wrangling the data,
filtering, subsetting, selecting columns, adding new columns or
modifying existing ones. All these methods can be chained one after
another and at the end of the procedure check if there has been any
errors by the DataFrame Err() method.

```
a = a.Rename("Origin", "Country").
    Filter(df.F{"Age", "<", 50}).
    Filter(df.F{"Origin", "==", "United States"}).
    Select("Id", "Origin", "Date").
    Subset([]int{1, 3})
if a.Err() != nil {
    log.Fatal("Oh noes!")
}
```

License
-------
Copyright 2016 Alejandro Sanchez Brotons

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You may
obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

[1]: https://github.com/gonum
[2]: https://github.com/kniren/gota
