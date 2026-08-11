package asyncapiclient

import "encoding/json"

// Binding objects remain open to protocols this client does not know yet.
// Built-in drivers get typed views of the bindings they implement; installed
// drivers receive every other entry unchanged through DriverRequest.

func (b *channelBindings) UnmarshalJSON(data []byte) error {
	raw, err := decodeBindingObject(data)
	if err != nil {
		return err
	}
	b.WS, err = takeBinding[wsChannelBinding](raw, "ws")
	if err != nil {
		return err
	}
	b.Raw = raw
	return nil
}

func (b channelBindings) MarshalJSON() ([]byte, error) {
	return encodeBindingObject(b.Raw, "ws", b.WS)
}

func (b *operationBindings) UnmarshalJSON(data []byte) error {
	raw, err := decodeBindingObject(data)
	if err != nil {
		return err
	}
	b.HTTP, err = takeBinding[httpOperationBinding](raw, "http")
	if err != nil {
		return err
	}
	b.Raw = raw
	return nil
}

func (b operationBindings) MarshalJSON() ([]byte, error) {
	return encodeBindingObject(b.Raw, "http", b.HTTP)
}

func (b *messageBindings) UnmarshalJSON(data []byte) error {
	raw, err := decodeBindingObject(data)
	if err != nil {
		return err
	}
	b.HTTP, err = takeBinding[httpMessageBinding](raw, "http")
	if err != nil {
		return err
	}
	b.Raw = raw
	return nil
}

func (b messageBindings) MarshalJSON() ([]byte, error) {
	return encodeBindingObject(b.Raw, "http", b.HTTP)
}

func decodeBindingObject(data []byte) (map[string]json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func takeBinding[T any](raw map[string]json.RawMessage, name string) (*T, error) {
	data, present := raw[name]
	if !present {
		return nil, nil
	}
	var value *T
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func encodeBindingObject(preserved map[string]json.RawMessage, knownName string, known any) ([]byte, error) {
	raw := make(map[string]json.RawMessage, len(preserved)+1)
	for name, value := range preserved {
		raw[name] = append(json.RawMessage(nil), value...)
	}
	if _, preservedKnown := raw[knownName]; !preservedKnown && known != nil {
		data, err := json.Marshal(known)
		if err != nil {
			return nil, err
		}
		raw[knownName] = data
	}
	return json.Marshal(raw)
}
