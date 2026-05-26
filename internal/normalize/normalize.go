// Package normalize приводит поисковые запросы к каноническому виду.
// Используется и window (горячий путь Record), и stoplist (Add/Remove),
// поэтому вынесен в отдельный пакет, а не дублируется.
package normalize

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

// Query приводит сырой поисковый запрос к нормализованному виду:
//  1. Unicode NFC — корректный lowercase кириллицы, лигатур и т.д.
//  2. Lowercase
//  3. Collapse spaces (strings.Fields обрабатывает все Unicode-пробелы)
//  4. Trim (неявно через Fields→Join)
//
// Пустая строка — сигнал отбросить событие.
func Query(s string) string {
	if s == "" {
		return ""
	}
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}
