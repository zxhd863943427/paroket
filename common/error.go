package common

import "fmt"

var (
	ErrObjectNotFound         = fmt.Errorf("object not found")
	ErrTableNotFound          = fmt.Errorf("table not found")
	ErrAttributeClassNotFound = fmt.Errorf("attribute class not found")
	ErrAttributeNotFound      = fmt.Errorf("attribute not found")
)
