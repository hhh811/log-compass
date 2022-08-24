package msg

import "time"

// basic log item
type Message struct {
	CollectTime time.Time `json:"time"`    // time receive this log message
	Creator     string    `json:"creator"` // where this log came from
	Content     string    `json:"content"` // log content
}
