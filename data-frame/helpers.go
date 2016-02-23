package df

// inStringSlice finds if a given string is containd on a []string
func inStringSlice(str string, s []string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}
