package main

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"time"
)

type Config struct {
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	FromDate   time.Time
	ToDate     time.Time
	Workers    int
}

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
	EntityID      string `gorm:"index"`
	EntityType    string
}

type StringArray []string

func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return "{}", nil
	}
	return "{" + strings.Join(a, ",") + "}", nil
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
