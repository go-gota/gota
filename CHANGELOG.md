# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
- Getter and setter methods for the column names of a DataFrame
- Bool column type has been made available
- New ToBool() interface
- A `column` now can now if any of it's elements is NA and a list of
  said NA elements ([]bool).

### Changed
- The `cell` interface has changed. ToInteger() and ToFloat() now
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

[0.1.0]:https://github.com/kniren/gota/compare/v0.1.0...v0.1.0
[0.2.0]:https://github.com/kniren/gota/compare/v0.1.0...v0.2.0
[0.2.1]:https://github.com/kniren/gota/compare/v0.2.0...v0.2.1
