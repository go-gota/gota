package df

//func TestNew(t *testing.T) {
//df, err := New(
//C{"A", Strings("aa", "b")},
//C{"B", Strings("a", "bbb")},
//)
//if err != nil {
//t.Error("Error when creating DataFrame:", err)
//}
//expected := "   A   B    \n\n0: aa  a    \n1: b   bbb  \n"
//received := fmt.Sprint(df)
//if expected != received {
//t.Error(
//"DataFrame created by New() is not correct",
//"Expected:\n",
//expected, "\n",
//"Received:\n",
//received,
//)
//}

//df, err = New()
//if err == nil {
//t.Error("Error when creating DataFrame not being thrown")
//}

//df, err = New(
//C{"A", Strings("a", "b")},
//C{"B", Strings("a", "b", "c")},
//)
//if err == nil {
//t.Error("Error when creating DataFrame not being thrown")
//}

//df, err = New(
//C{"A", Strings()},
//C{"B", Strings("a", "b", "c")},
//)
//if err == nil {
//t.Error("Error when creating DataFrame not being thrown")
//}
//}

//func TestDataFrame_LoadData(t *testing.T) {
//data := [][]string{
//[]string{"A", "B", "C", "D"},
//[]string{"1", "2", "3", "4"},
//[]string{"5", "6", "7", "8"},
//}

//// Test correct data loading
//df := DataFrame{}
//df.LoadData(data)
//expected := "   A  B  C  D  \n\n0: 1  2  3  4  \n1: 5  6  7  8  \n"
//received := fmt.Sprint(df)
//if expected != received {
//t.Error(
//"DataFrame loaded data incorrectly",
//"Expected:\n",
//expected, "\n",
//"Received:\n",
//received,
//)
//}

//// Test nil data loading
//err := df.LoadData(nil)
//if err == nil {
//t.Error("DataFrame should have failed")
//}

//// Test empty headers
//data = [][]string{
//[]string{"", "", "", ""},
//[]string{"1", "2", "3", "4"},
//[]string{"5", "6", "7", "8"},
//}
//df.LoadData(data)
//expectedColnames := fmt.Sprint([]string{"V0", "V1", "V2", "V3"})
//receivedColnames := fmt.Sprint(df.colNames)
//if expectedColnames != receivedColnames {
//t.Error(
//"Colnames not being generated properly",
//"Expected:\n",
//expectedColnames, "\n",
//"Received:\n",
//receivedColnames,
//)
//}

//// Test duplicated headers
//data = [][]string{
//[]string{"A", "B", "A", "C"},
//[]string{"1", "2", "3", "4"},
//[]string{"5", "6", "7", "8"},
//}
//err = df.LoadData(data)
//if err == nil {
//t.Error("Duplicated headers but no error")
//}
//}
