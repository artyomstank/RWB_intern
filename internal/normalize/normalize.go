package normalize

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

func Query(s string) string {
	if s == "" {
		return ""
	}
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}
