package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTP     HTTPConfig
	Kafka    KafkaConfig
	StopList StopListConfig
	TopN     TopNConfig
}

type HTTPConfig struct {
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
	Group   string
}

type StopListConfig struct {
	DBPath string
}

type TopNConfig struct {
	N          int
	RefreshTTL time.Duration
}

// MustLoad читает конфигурацию из переменных окружения.
// При критической ошибке логирует и завершает процесс.
func MustLoad() Config {
	return Config{
		HTTP: HTTPConfig{
			Addr:         getEnv("TRENDING_HTTP_ADDR", ":8080"),
			ReadTimeout:  getDuration("TRENDING_HTTP_READ_TIMEOUT", 5*time.Second),
			WriteTimeout: getDuration("TRENDING_HTTP_WRITE_TIMEOUT", 10*time.Second),
			IdleTimeout:  getDuration("TRENDING_HTTP_IDLE_TIMEOUT", 120*time.Second),
		},
		Kafka: KafkaConfig{
			Brokers: strings.Split(getEnv("TRENDING_KAFKA_BROKERS", "localhost:9092"), ","),
			Topic:   getEnv("TRENDING_KAFKA_TOPIC", "search.events"),
			Group:   getEnv("TRENDING_KAFKA_GROUP", "trending-v1"),
		},
		StopList: StopListConfig{
			DBPath: getEnv("TRENDING_STOPLIST_DB", "/data/stoplist.db"),
		},
		TopN: TopNConfig{
			N:          getInt("TRENDING_TOPN_SIZE", 10),
			RefreshTTL: getDuration("TRENDING_TOPN_REFRESH", 2*time.Second),
		},
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
		slog.Warn("invalid int env var, using default", "key", key, "default", def)
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
		slog.Warn("invalid duration env var, using default", "key", key, "default", def)
	}
	return def
}
