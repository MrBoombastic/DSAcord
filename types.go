package main

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"time"
)

type Config struct {
	DBHost     string `env:"DB_HOST" envDefault:"localhost"`
	DBPort     int    `env:"DB_PORT" envDefault:"5432"`
	DBUser     string `env:"DB_USER" envDefault:"postgres"`
	DBPassword string `env:"DB_PASSWORD" envDefault:"root"`
	DBName     string `env:"DB_NAME" envDefault:"dsacord"`
	FromDate   time.Time
	ToDate     time.Time
}

// Decision model
type Decision struct {
	UUID                           string      `gorm:"primaryKey;type:uuid"`
	DecisionVisibility             StringArray `gorm:"type:text[]"`
	DecisionVisibilityOther        string
	EndDateVisibilityRestriction   *time.Time
	DecisionMonetary               StringArray `gorm:"type:text[]"`
	DecisionMonetaryOther          string
	EndDateMonetaryRestriction     *time.Time
	DecisionProvision              StringArray `gorm:"type:text[]"`
	EndDateServiceRestriction      *time.Time
	DecisionAccount                StringArray `gorm:"type:text[]"`
	EndDateAccountRestriction      *time.Time
	AccountType                    string
	DecisionGround                 string
	DecisionGroundReferenceURL     string
	IllegalContentLegalGround      string
	IllegalContentExplanation      string
	IncompatibleContentGround      string
	IncompatibleContentExplanation string
	IncompatibleContentIllegal     sql.NullBool
	Category                       string
	CategoryAddition               string
	CategorySpecification          StringArray `gorm:"type:text[]"`
	CategorySpecificationOther     string
	ContentType                    StringArray `gorm:"type:text[]"`
	ContentTypeOther               string
	ContentLanguage                string
	ContentDate                    *time.Time
	TerritorialScope               StringArray `gorm:"type:text[]"`
	ApplicationDate                *time.Time
	DecisionFacts                  string
	SourceType                     string
	SourceIdentity                 string
	AutomatedDetection             sql.NullBool
	AutomatedDecision              string
	PlatformName                   string
	PlatformUID                    string
	CreatedAt                      time.Time

	// Parsed PlatformUID fields
	SnowflakeTime time.Time
	EntityID      string
	EntityType    string
}

// StringArray is a custom type for Postgres text[]
type StringArray []string

func (a *StringArray) Value() (driver.Value, error) {
	if a == nil {
		return "{}", nil
	}
	return "{" + strings.Join(*a, ",") + "}", nil
}

func (a *StringArray) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}
	switch src := src.(type) {
	case string:
		s := strings.Trim(src, "{}")
		if s == "" {
			*a = nil
			return nil
		}
		*a = strings.Split(s, ",")
		return nil
	default:
		return errors.New("incompatible type for StringArray")
	}
}
