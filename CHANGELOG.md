# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
- Added a lot of unit tests

### Changed
- The base types are now df.String, df.Int, df.Float and df.Date
- Restructured the project in different files
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
