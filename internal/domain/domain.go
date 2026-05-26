package domain

import "time"

// SearchEvent — контракт сообщения из Kafka.
// Ключ сообщения (message key) = нормализованный текст запроса,
// что гарантирует попадание всех событий одного запроса в одну партицию.
type SearchEvent struct {
	// Query — сырой текст поискового запроса до нормализации.
	// Нормализацию (lowercase, trim, NFC) делаем на своей стороне.
	Query string `json:"query"`

	// UserID — UUID аутентифицированного пользователя или "anon:<session_id>".
	// Основной input для HyperLogLog при подсчёте уникальных пользователей.
	UserID string `json:"user_id"`

	// Ts — время события на стороне источника (event time, не processing time).
	// RFC3339Nano: наносекунды нужны для корректного упорядочивания.
	Ts time.Time `json:"ts"`

	// SessionID — ID браузерной сессии. Fallback-идентификатор для anti-gaming
	// когда user_id недоступен: боты ротируют аккаунты, но сессия стабильна.
	SessionID string `json:"session_id"`

	// IPHash — SHA-256(client_ip)[:16] hex. Не храним сам IP (GDPR).
	// Зафиксирован в контракте для будущего третьего уровня защиты от бот-ферм.
	IPHash string `json:"ip_hash"`

	// Platform, Locale, Source — опциональные поля для аналитики.
	Platform string `json:"platform,omitempty"`
	Locale   string `json:"locale,omitempty"`
	Source   string `json:"source,omitempty"`
}

// TopEntry — одна позиция в ответе GET /api/v1/top.
type TopEntry struct {
	Rank        int    `json:"rank"`
	Query       string `json:"query"`
	Count       int32  `json:"count"`
	UniqueUsers uint64 `json:"unique_users"`
}
