// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// HeartbeatMessage heartbeat message
//
// swagger:model HeartbeatMessage
type HeartbeatMessage struct {
	idField int32

	HeartbeatMessageAllOf1
}

// ID gets the id of this subtype
func (m *HeartbeatMessage) ID() int32 {
	return m.idField
}

// SetID sets the id of this subtype
func (m *HeartbeatMessage) SetID(val int32) {
	m.idField = val
}

// Op gets the op of this subtype
func (m *HeartbeatMessage) Op() string {
	return "heartbeat"
}

// SetOp sets the op of this subtype
func (m *HeartbeatMessage) SetOp(val string) {
}

// UnmarshalJSON unmarshals this object with a polymorphic type from a JSON structure
func (m *HeartbeatMessage) UnmarshalJSON(raw []byte) error {
	var data struct {
		HeartbeatMessageAllOf1
	}
	buf := bytes.NewBuffer(raw)
	dec := json.NewDecoder(buf)
	dec.UseNumber()

	if err := dec.Decode(&data); err != nil {
		return err
	}

	var base struct {
		/* Just the base type fields. Used for unmashalling polymorphic types.*/

		ID int32 `json:"id,omitempty"`

		Op string `json:"op,omitempty"`
	}
	buf = bytes.NewBuffer(raw)
	dec = json.NewDecoder(buf)
	dec.UseNumber()

	if err := dec.Decode(&base); err != nil {
		return err
	}

	var result HeartbeatMessage

	result.idField = base.ID

	if base.Op != result.Op() {
		/* Not the type we're looking for. */
		return errors.New(422, "invalid op value: %q", base.Op)
	}
	result.HeartbeatMessageAllOf1 = data.HeartbeatMessageAllOf1

	*m = result

	return nil
}

// MarshalJSON marshals this object with a polymorphic type to a JSON structure
func (m HeartbeatMessage) MarshalJSON() ([]byte, error) {
	var b1, b2, b3 []byte
	var err error
	b1, err = json.Marshal(struct {
		HeartbeatMessageAllOf1
	}{

		HeartbeatMessageAllOf1: m.HeartbeatMessageAllOf1,
	})
	if err != nil {
		return nil, err
	}
	b2, err = json.Marshal(struct {
		ID int32 `json:"id,omitempty"`

		Op string `json:"op,omitempty"`
	}{

		ID: m.ID(),

		Op: m.Op(),
	})
	if err != nil {
		return nil, err
	}

	return swag.ConcatJSON(b1, b2, b3), nil
}

// Validate validates this heartbeat message
func (m *HeartbeatMessage) Validate(formats strfmt.Registry) error {
	var res []error

	// validation for a type composition with HeartbeatMessageAllOf1

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// ContextValidate validate this heartbeat message based on the context it is used
func (m *HeartbeatMessage) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	// validation for a type composition with HeartbeatMessageAllOf1

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// MarshalBinary interface implementation
func (m *HeartbeatMessage) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *HeartbeatMessage) UnmarshalBinary(b []byte) error {
	var res HeartbeatMessage
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// HeartbeatMessageAllOf1 heartbeat message all of1
//
// swagger:model HeartbeatMessageAllOf1
type HeartbeatMessageAllOf1 interface{}