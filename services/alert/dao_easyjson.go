// Code generated by easyjson for marshaling/unmarshaling. DO NOT EDIT.

package alert

import (
	json "encoding/json"
	time "time"

	easyjson "github.com/mailru/easyjson"
	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
)

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjson7be57abeDecodeGithubComInfluxdataKapacitorServicesAlert(in *jlexer.Lexer, out *TopicState) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "topic":
			out.Topic = string(in.String())
		case "event-states":
			if in.IsNull() {
				in.Skip()
			} else {
				in.Delim('{')
				out.EventStates = make(map[string]EventState)
				for !in.IsDelim('}') {
					key := string(in.String())
					in.WantColon()
					var v1 EventState
					(v1).UnmarshalEasyJSON(in)
					(out.EventStates)[key] = v1
					in.WantComma()
				}
				in.Delim('}')
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjson7be57abeEncodeGithubComInfluxdataKapacitorServicesAlert(out *jwriter.Writer, in TopicState) {
	out.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"topic\":"
		out.RawString(prefix[1:])
		out.String(string(in.Topic))
	}
	{
		const prefix string = ",\"event-states\":"
		out.RawString(prefix)
		if in.EventStates == nil && (out.Flags&jwriter.NilMapAsEmpty) == 0 {
			out.RawString(`null`)
		} else {
			out.RawByte('{')
			v2First := true
			for v2Name, v2Value := range in.EventStates {
				if v2First {
					v2First = false
				} else {
					out.RawByte(',')
				}
				out.String(string(v2Name))
				out.RawByte(':')
				(v2Value).MarshalEasyJSON(out)
			}
			out.RawByte('}')
		}
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v TopicState) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjson7be57abeEncodeGithubComInfluxdataKapacitorServicesAlert(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v TopicState) MarshalEasyJSON(w *jwriter.Writer) {
	easyjson7be57abeEncodeGithubComInfluxdataKapacitorServicesAlert(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *TopicState) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjson7be57abeDecodeGithubComInfluxdataKapacitorServicesAlert(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *TopicState) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjson7be57abeDecodeGithubComInfluxdataKapacitorServicesAlert(l, v)
}
func easyjson7be57abeDecodeGithubComInfluxdataKapacitorServicesAlert1(in *jlexer.Lexer, out *EventState) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "message":
			out.Message = string(in.String())
		case "details":
			out.Details = string(in.String())
		case "time":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Time).UnmarshalJSON(data))
			}
		case "duration":
			out.Duration = time.Duration(in.Int64())
		case "level":
			if data := in.UnsafeBytes(); in.Ok() {
				in.AddError((out.Level).UnmarshalText(data))
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjson7be57abeEncodeGithubComInfluxdataKapacitorServicesAlert1(out *jwriter.Writer, in EventState) {
	out.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"message\":"
		out.RawString(prefix[1:])
		out.String(string(in.Message))
	}
	{
		const prefix string = ",\"details\":"
		out.RawString(prefix)
		out.String(string(in.Details))
	}
	{
		const prefix string = ",\"time\":"
		out.RawString(prefix)
		out.Raw((in.Time).MarshalJSON())
	}
	{
		const prefix string = ",\"duration\":"
		out.RawString(prefix)
		out.Int64(int64(in.Duration))
	}
	{
		const prefix string = ",\"level\":"
		out.RawString(prefix)
		out.RawText((in.Level).MarshalText())
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v EventState) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjson7be57abeEncodeGithubComInfluxdataKapacitorServicesAlert1(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v EventState) MarshalEasyJSON(w *jwriter.Writer) {
	easyjson7be57abeEncodeGithubComInfluxdataKapacitorServicesAlert1(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *EventState) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjson7be57abeDecodeGithubComInfluxdataKapacitorServicesAlert1(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *EventState) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjson7be57abeDecodeGithubComInfluxdataKapacitorServicesAlert1(l, v)
}
