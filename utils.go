package elasticsearchutil

// stringOrNil returns the given string or nil when empty
func stringOrNil(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}
