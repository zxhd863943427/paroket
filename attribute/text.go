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

func (t *TextAttribute) GetJSON() string {
	return fmt.Sprintf(`{"id": "%s","type": "%s", "value": "%s"}`, t.id, AttributeTypeText, t.value)
}

func (t *TextAttribute) GetType() string {
	return AttributeTypeText
}
