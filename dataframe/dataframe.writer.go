package dataframe

//Writer Writer interface for
type Writer interface {
	Write(DataFrame) error
}
