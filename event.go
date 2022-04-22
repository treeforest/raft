package raft

type Event interface {
	Type() string
	Source() interface{}
	Value() interface{}
	PrevValue() interface{}
}

type event struct {
	typ       string
	source    interface{}
	value     interface{}
	prevValue interface{}
}

func newEvent(typ string, value interface{}, prevValue interface{}) *event {
	return &event{
		typ:       typ,
		value:     value,
		prevValue: prevValue,
	}
}

func (e *event) Type() string {
	return e.typ
}

func (e *event) Source() interface{} {
	return e.source
}

func (e *event) Value() interface{} {
	return e.value
}

func (e *event) PrevValue() interface{} {
	return e.prevValue
}
