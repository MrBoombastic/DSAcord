package main

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/schollz/progressbar/v3"
	"gorm.io/gorm"
)

func downloadWithBackoff(url string) ([]byte, error) {
	var data []byte
	operation := func() error {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("bad status: %s", resp.Status)
		}

		data, err = io.ReadAll(resp.Body)
		return err
	}

	err := backoff.Retry(operation, backoff.NewExponentialBackOff())
	return data, err
}

func processZip(data []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}

	var allDecisions []Decision
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}

		defer rc.Close()

		// Nested zip
		if filepath.Ext(f.Name) == ".zip" {
			nestedData, _ := io.ReadAll(rc)
			nestedZip, err := zip.NewReader(bytes.NewReader(nestedData), int64(len(nestedData)))
			if err != nil {
				return err
			}
			for _, nf := range nestedZip.File {
				if err := readCSV(nf, &allDecisions); err != nil {
					return err
				}
			}
		} else if filepath.Ext(f.Name) == ".gz" {
			gzr, _ := gzip.NewReader(rc)
			defer gzr.Close()
			if err := readCSVFile(gzr, &allDecisions); err != nil {
				return err
			}
		} else {
			if err := readCSVFile(rc, &allDecisions); err != nil {
				return err
			}
		}
	}

	bar := progressbar.NewOptions(len(allDecisions),
		progressbar.OptionSetDescription("Inserting decisions..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionShowBytes(false),
		progressbar.OptionThrottle(300*time.Millisecond),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
	)

	// Bulk insert with transaction
	batchSize := 1000
	return db.Transaction(func(tx *gorm.DB) error {
		for i := 0; i < len(allDecisions); i += batchSize {
			end := i + batchSize
			if end > len(allDecisions) {
				end = len(allDecisions)
			}
			batch := allDecisions[i:end]
			if err := tx.CreateInBatches(batch, batchSize).Error; err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			bar.Add(len(batch))
		}
		return nil
	})
}

func readCSV(f *zip.File, allDecisions *[]Decision) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	return readCSVFile(rc, allDecisions)
}

func readCSVFile(r io.Reader, allDecisions *[]Decision) error {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	headers := make(map[string]int)
	for i, h := range records[0] {
		headers[h] = i
	}

	for _, record := range records[1:] {
		decision := parseDecision(headers, record)
		*allDecisions = append(*allDecisions, decision)
	}

	return nil
}

func parseDecision(headers map[string]int, record []string) Decision {
	get := func(key string) string {
		idx, ok := headers[key]
		if !ok || idx >= len(record) {
			return ""
		}
		return record[idx]
	}

	parseTime := func(value string) *time.Time {
		if value == "" {
			return nil
		}
		t, err := time.Parse("2006-01-02 15:04:05", value)
		if err != nil {
			return nil
		}
		return &t
	}

	snowflakeTime, entityID, entityType := parsePlatformUID(get("platform_uid"))

	return Decision{
		UUID:                           get("uuid"),
		DecisionVisibility:             parseArrayField(get("decision_visibility")),
		DecisionVisibilityOther:        get("decision_visibility_other"),
		EndDateVisibilityRestriction:   parseTime(get("end_date_visibility_restriction")),
		DecisionMonetary:               parseArrayField(get("decision_monetary")),
		DecisionMonetaryOther:          get("decision_monetary_other"),
		EndDateMonetaryRestriction:     parseTime(get("end_date_monetary_restriction")),
		DecisionProvision:              parseArrayField(get("decision_provision")),
		EndDateServiceRestriction:      parseTime(get("end_date_service_restriction")),
		DecisionAccount:                parseArrayField(get("decision_account")),
		EndDateAccountRestriction:      parseTime(get("end_date_account_restriction")),
		AccountType:                    get("account_type"),
		DecisionGround:                 get("decision_ground"),
		DecisionGroundReferenceURL:     get("decision_ground_reference_url"),
		IllegalContentLegalGround:      get("illegal_content_legal_ground"),
		IllegalContentExplanation:      get("illegal_content_explanation"),
		IncompatibleContentGround:      get("incompatible_content_ground"),
		IncompatibleContentExplanation: get("incompatible_content_explanation"),
		Category:                       get("category"),
		CategoryAddition:               get("category_addition"),
		CategorySpecification:          parseArrayField(get("category_specification")),
		CategorySpecificationOther:     get("category_specification_other"),
		ContentType:                    parseArrayField(get("content_type")),
		ContentTypeOther:               get("content_type_other"),
		ContentLanguage:                get("content_language"),
		ContentDate:                    parseTime(get("content_date")),
		TerritorialScope:               parseArrayField(get("territorial_scope")),
		ApplicationDate:                parseTime(get("application_date")),
		DecisionFacts:                  get("decision_facts"),
		SourceType:                     get("source_type"),
		SourceIdentity:                 get("source_identity"),
		AutomatedDetection:             parseBool(get("automated_detection")),
		AutomatedDecision:              get("automated_decision"),
		PlatformName:                   get("platform_name"),
		PlatformUID:                    get("platform_uid"),
		CreatedAt:                      *parseTime(get("created_at")),
		SnowflakeTime:                  snowflakeTime,
		EntityID:                       entityID,
		EntityType:                     entityType,
	}
}

func parseArrayField(value string) StringArray {
	if value == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(value), &arr); err == nil {
		return arr
	}
	return StringArray{value}
}

func parseBool(value string) sql.NullBool {
	switch strings.ToLower(value) {
	case "yes":
		return sql.NullBool{Bool: true, Valid: true}
	case "no":
		return sql.NullBool{Bool: false, Valid: true}
	default:
		return sql.NullBool{Valid: false}
	}
}

func parsePlatformUID(platformUID string) (time.Time, string, string) {
	parts := strings.Split(platformUID, "-")
	if len(parts) >= 3 {
		snowflake := parts[0]
		id := parts[1]
		typ := parts[2]

		sf, err := parseSnowflake(snowflake)
		if err != nil {
			return time.Time{}, id, typ
		}
		return sf, id, typ
	}
	return time.Time{}, "", ""
}

func parseSnowflake(snowflake string) (time.Time, error) {
	sfInt, err := strconv.ParseInt(snowflake, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	ms := (sfInt >> 22) + 1420070400000
	return time.UnixMilli(ms), nil
}
