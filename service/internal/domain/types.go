package domain

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// Event is the device envelope. Optional fields are pointers so a missing
// value stays distinguishable from a zero value.
type Event struct {
	DeviceID string   `json:"device_id"`
	RoomID   string   `json:"room_id"`
	Type     string   `json:"type"`
	TsRaw    string   `json:"ts"`
	Seq      int64    `json:"seq"`
	InRoom   *bool    `json:"in_room,omitempty"`
	Conf     *float64 `json:"confidence,omitempty"`
	Magn     *float64 `json:"magnitude,omitempty"`
	State    *string  `json:"state,omitempty"`
	Rssi     *int     `json:"rssi,omitempty"`

	Ts time.Time `json:"-"` // parsed TsRaw, authoritative time

	IngestTs time.Time `json:"-"` // HTTP receive time, never persisted
}

// ValidType reports whether t is a known event type.
func ValidType(t string) bool {
	switch t {
	case "heartbeat", "presence", "motion", "sleep_state", "fall_warn", "net_status":
		return true
	}
	return false
}

// ValidState reports whether s is a known sleep_state value.
func ValidState(s string) bool {
	switch s {
	case "asleep", "awake", "unknown":
		return true
	}
	return false
}

func validUnit(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 && v <= 1
}

// ValidateTypeFields checks per-type required fields per docs/event_schema.md.
// Returns ok=false with a short reason when the event must be rejected.
func ValidateTypeFields(typ string, inRoom *bool, magn *float64, state *string, conf *float64, rssi *int) (bool, string) {
	switch typ {
	case "heartbeat":
		return true, ""
	case "presence":
		if inRoom == nil {
			return false, "presence missing in_room"
		}
		return true, ""
	case "motion":
		if magn == nil {
			return false, "motion missing magnitude"
		}
		if !validUnit(*magn) {
			return false, "motion magnitude must be 0..1"
		}
		return true, ""
	case "sleep_state":
		if state == nil || !ValidState(*state) {
			return false, "sleep_state state must be asleep|awake|unknown"
		}
		return true, ""
	case "fall_warn":
		if conf == nil {
			return false, "fall_warn missing confidence"
		}
		if !validUnit(*conf) {
			return false, "fall_warn confidence must be 0..1"
		}
		return true, ""
	case "net_status":
		if rssi == nil {
			return false, "net_status missing rssi"
		}
		return true, ""
	}
	return false, "unknown type"
}

// IsPriority reports whether t goes to the prio channel (fall_warn only).
func IsPriority(t string) bool {
	return t == "fall_warn"
}

// Alarm is one deduplicated fall warning with its original timestamp.
type Alarm struct {
	EventID    string    `json:"event_id"`
	DeviceID   string    `json:"device_id"`
	RoomID     string    `json:"room_id"`
	Ts         time.Time `json:"-"`
	TsRaw      string    `json:"ts"`
	Confidence float64   `json:"confidence"`

	IngestTs time.Time `json:"-"` // server receive time, never persisted
}

// ParseTime accepts RFC3339Nano, plain RFC3339, unix seconds, or "0"/""
// meaning beginning of time.
func ParseTime(s string) (time.Time, bool) {
	if s == "" || s == "0" {
		return time.Time{}, true
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	trimmed := strings.TrimSpace(s)
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return time.Time{}, false
		}
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), true
	}
	return time.Time{}, false
}
