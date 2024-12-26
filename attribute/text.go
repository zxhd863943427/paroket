package attribute

import (
	"fmt"

	"github.com/google/uuid"
)

type TextAttribute struct {
	id    uuid.UUID
	value string
}

func (t *TextAttribute) GetId() uuid.UUID {
	return t.id
}

func (t *TextAttribute) GetValue() string {
	return fmt.Sprintf(`{id: "%s",type: "text", value: "%s"}`, t.id, t.value)
}
