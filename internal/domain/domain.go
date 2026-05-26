package domain

import "time"

type SearchEvent struct {
	Query string `json:"query"`

	UserID string `json:"user_id"`

	Ts time.Time `json:"ts"`

	SessionID string `json:"session_id"`

	IPHash string `json:"ip_hash"`

	Platform string `json:"platform,omitempty"`
	Locale   string `json:"locale,omitempty"`
	Source   string `json:"source,omitempty"`
}

type TopEntry struct {
	Rank        int    `json:"rank"`
	Query       string `json:"query"`
	Count       int32  `json:"count"`
	UniqueUsers uint64 `json:"unique_users"`
}
