# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.11.0] - 2021-06-27
### Added
- Rolling window Mean and StdDev
- GroupBy and Aggregate
- Numeric column index
- Read HTML tables
- extra checks for TravisCI
- Combining filters with AND
- User-defined filters
- Concatination of Dataframes

### Changed
- Make fixColnames faster
- Use Go 1.16
- Update dependencies

### Fixed
- Linter issues
- Failing tests

## [0.10.1] - 2019-11-08
### Fixed
- LoadRecords printing type debug information
- Missing closing brackets in series.go
- Fix gonum import path in dataframe_test

## [0.10.0] - 2019-11-08
### Changed
- Merged dev branch changes from multiple collaborators (Sam Zaydel, Kyle
  Ellrott, Daniela Petruzalek, Christoph Laaber).

## [0.9.0] - 2016-10-03
### Added
- Additional method to load arbitrary struct slices to DataFrames (Juan Álvarez)
- New LoadOption Names to set initial column names (Sander van Harmelen).
- Parser option for csv delimiter (Kyle Ellrott)
- New Describe method for reporting summary statistics (Daniela Petruzalek)

### Changed
- Improve the performance of multiple operations.
- Code cleanup for better consistency (Sander van Harmelen)
- Renamed 'Deselect' function to 'Drop' (Ben Marshall)

## [0.8.0] - 2016-12-12
### Added
- Series.Order method and tests.
- Series.IsNaN method and tests.
- DataFrame.Arrange method and tests.
- DataFrame.Capply method and tests.
- DataFrame.Rapply method and tests.
- Benchmarks for several operations on both the `series` and
  `dataframe` packages.
- Many optimizations that increase the performance dramatically.
- New LoadOption where the elements to be parsed as NaN from string
  can be selected.
- Gota can now return an implementation of `gonum/mat64.Matrix`
  interface via `DataFrame.Matrix()` and load a `mat64.Matrix` via
  `dataframe.LoadMatrix()`.

### Changed
- elementInterface is now exported as Element.
- Split element.go into separate files for the implementations of the
  Element interface.
- LoadOptions API has been renamed for better documentation via `godoc`.
- `Series.Set` and `DataFrame.Set` now modify the structure in place
  for performance considerations. If one wants to use the old
  behaviour, it is suggested to use `DataFrame.Copy().Set(...)`
  instead of `DataFrame.Set(...)`.
- `DataFrame.Dim` has been changed to `DataFrame.Dims` for consistency
  with the `mat64.Matrix` interface.
- When printing a large `DataFrame` now the behaviour of the stringer
  interface is much nicer, showing only the first 10 rows and limiting
  the number of characters that can be shown by line

### Removed
- Some unused functions from the helpers.go file.

### Fix
- Linter errors.
- stringElement.Float now returns NaN instead of 0 when applicable.
- Autorenaming column names when `hasHeaders == false` now is
  consistent with the autorename used with `dataframe.New`
- Bug where duplicated column names were not been assigned consecutive
  suffix numbers if the number of duplicates was greater than two.

## [0.7.0] - 2016-11-27
### Added
- Many more table tests for both `series` and `dataframe`
- Set method for `Series` and `DataFrame`
- When loading data from CSV, JSON, or Records, different
  `LoadOptions` can now be configured. This includes assigning
  a default type, manually specifying the column types and others.
- More documentation for previously undocumented functions.

### Changed
- The project has been restructured on separated `dataframe` and
  `series` packages.
- Reviewed entire `Series` codebase for better style and
  maintainability.
- `DataFrame.Select` now accepts several types of indexes
- Error messages are now more consistent.
- The standard way of checking for errors on both `series` and
  `dataframe` is to check the `Err` field on each structure.
- `ReadCSV`/`ReadJSON` and `WriteCSV`/`WriteJSON` now accept
  `io.Reader` and `io.Writer` respectively.
- Updated README with the new changes.

### Removed
- Removed unnecessary abstraction layer on `Series.elements`

## [0.6.0] - 2016-10-29
### Added
- InnerJoin, CrossJoin, RightJoin, LeftJoin, OuterJoin functions

### Changed
- More code refactoring for easier maintenance and management
- Add more documentation to the exported functions
- Remove unnecessary methods and structures from the exported API

### Removed
- colnames and coltypes from the DataFrame structure

## [0.5.0] - 2016-08-09
### Added
- Read and write DataFrames from CSV, JSON, []map[string]interface{},
  [][]string.
- New constructor for DataFrame accept Series and NamedSeries as
  arguments.
- Subset, Select, Rename, Mutate, Filter, RBind and CBind methods
- Much Better error handling

### Changed
- Almost complete rewrite of DataFrame code.
- Now using Series as first class citizens and building blocks for
  DataFrames.

### Removed
- Merge/Join functions have been temporarily removed to be adapted to
  the new architecture.
- Cell interface for allowing custom types into the system.

## [0.4.0] - 2016-02-18
### Added
- Getter methods for nrows and ncols.
- An InnerJoin function that performs an Inner Merge/Join of two
  DataFrames by the given keys.
- An RightJoin and LeftJoin functions that performs outer right/outer
  left joins of two DataFrames by the given keys.
- A CrossJoin function that performs an Cross Merge/Join of two
  DataFrames.
- Cell interface now have to implement the NA() method that will
  return a empty cell for the given type.
- Cell interface now have to implement a Copy method.

### Changed
- The `cell` interface is now exported: `Cell`.
- Cell method NA() is now IsNA().
- The function parseColumn is now a method.
- A number of fields and methods are now expoted.

### Fixed
- Now ensuring that generated subsets are in fact new copies entirely,
  not copying pointers to the same memory address.

## [0.3.0] - 2016-02-18
### Added
- Getter and setter methods for the column names of a DataFrame
- Bool column type has been made available
- New Bool() interface
- A `column` now can now if any of it's elements is NA and a list of
  said NA elements ([]bool).

### Changed
- Renamed `cell` interface elements to be more idiomatic:
    - ToInteger() is now Int()
    - ToFloat() is now Float()
- The `cell` interface has changed. Int() and Float() now
  return pointers instead of values to prevent future conflicts when
  returning an error. 
- The `cell` interface has changed. Checksum() [16]byte added.
- Using cell.Checksum() for identification of unique elements instead
  of raw strings.
- The `cell` interface has changed, now also requires ToBool() method.
- String type now does not contain a string, but a pointer to a string.

### Fixed
- Bool type constructor function Bools now parses `bool` and `[]bool`
  elements correctly.
- Int type constructor function Ints now parses `bool` and `[]bool`
  elements correctly.
- Float type constructor function Floats now parses `bool` and `[]bool`
  elements correctly.
- String type constructor function Strings now parses `bool` and `[]bool`
  elements correctly.

## [0.2.1] - 2016-02-14
### Fixed
- Fixed a bug when the maximum number of characters on a column was
  not being updated properly when subsetting.

## [0.2.0] - 2016-02-13
### Added
- Added a lot of unit tests

### Changed
- The base types are now `df.String`, `df.Int`, and `df.Float`.
- Restructured the project in different files.
- Refactored the project so that it will allow columns to be of any
  type as long as it complies with the necessary interfaces.


## [0.1.0] - 2016-02-06
### Added
- Load csv data to DataFrame.
- Parse data to four supported types: `int`, `float64`, `date`
  & `string`.
- Row/Column subsetting (Indexing, column names, row numbers, range).
- Unique/Duplicated row subsetting.
- DataFrame combinations by rows and columns (cbind/rbind).

[0.1.0]:https://github.com/go-gota/gota/compare/v0.1.0...v0.1.0
[0.2.0]:https://github.com/go-gota/gota/compare/v0.1.0...v0.2.0
[0.2.1]:https://github.com/go-gota/gota/compare/v0.2.0...v0.2.1
[0.3.0]:https://github.com/go-gota/gota/compare/v0.2.1...v0.3.0
[0.4.0]:https://github.com/go-gota/gota/compare/v0.3.0...v0.4.0
[0.5.0]:https://github.com/go-gota/gota/compare/v0.4.0...v0.5.0
[0.6.0]:https://github.com/go-gota/gota/compare/v0.5.0...v0.6.0
[0.7.0]:https://github.com/go-gota/gota/compare/v0.6.0...v0.7.0
[0.8.0]:https://github.com/go-gota/gota/compare/v0.7.0...v0.8.0
[0.9.0]:https://github.com/go-gota/gota/compare/v0.8.0...v0.9.0
[0.10.0]:https://github.com/go-gota/gota/compare/v0.9.0...v0.10.0
[0.10.1]:https://github.com/go-gota/gota/compare/v0.10.0...v0.10.1
