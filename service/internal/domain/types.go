package domain

import (
	"fmt"
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

	Ts time.Time `json:"-"` // parsed TsRaw, authoritative time
}

// ValidType reports whether t is a known event type.
func ValidType(t string) bool {
	switch t {
	case "heartbeat", "presence", "motion", "sleep_state", "fall_warn", "net_status":
		return true
	}
	return false
}

// Alarm is one deduplicated fall warning with its original timestamp.
type Alarm struct {
	EventID    string    `json:"event_id"`
	DeviceID   string    `json:"device_id"`
	RoomID     string    `json:"room_id"`
	Ts         time.Time `json:"-"`
	TsRaw      string    `json:"ts"`
	Confidence float64   `json:"confidence"`
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
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), true
	}
	return time.Time{}, false
}
