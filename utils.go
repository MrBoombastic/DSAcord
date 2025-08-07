package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/schollz/progressbar/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func downloadWithBackoff(url string) ([]byte, error) {
	var data []byte
	operation := func() error {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusForbidden {
			return backoff.Permanent(fmt.Errorf("forbidden or does not exist"))
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("bad status: %s", resp.Status)
		}

		data, err = io.ReadAll(resp.Body)
		return err
	}

	err := backoff.Retry(operation, backoff.NewExponentialBackOff())
	return data, err
}

func processZip(data []byte, bar *progressbar.ProgressBar) (allDecisions []Decision, err error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open zip file: %w", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, f := range zr.File {
		wg.Add(1)
		go func(file *zip.File) {
			defer wg.Done()
			localDecisions, err := readFileAndExtractDecisions(file)
			if err != nil {
				fmt.Printf("failed to read file %s: %v\n", file.Name, err)
				return
			}
			mu.Lock()
			allDecisions = append(allDecisions, localDecisions...)
			mu.Unlock()
		}(f)
	}
	wg.Wait()

	if numWorkers <= 1 {
		bar = progressbar.NewOptions(len(allDecisions),
			progressbar.OptionSetDescription("💾 Inserting decisions..."),
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionThrottle(300*time.Millisecond),
			progressbar.OptionSetWidth(30),
			progressbar.OptionSpinnerType(14),
		)
	}

	return
}

func processDecisions(allDecisions []Decision, bar *progressbar.ProgressBar, force bool) (err error) {
	batchSize := 1000 // do not increase this too much, it may cause postgres errors

	return db.Transaction(func(tx *gorm.DB) error {
		for i := 0; i < len(allDecisions); i += batchSize {
			end := i + batchSize
			if end > len(allDecisions) {
				end = len(allDecisions)
			}
			batch := allDecisions[i:end]

			if force || skipCheckingDuplicates {
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "uuid"}},
					UpdateAll: true,
				}).CreateInBatches(batch, batchSize).Error; err != nil {
					return err
				}
			} else {
				if err := tx.CreateInBatches(batch, batchSize).Error; err != nil {
					return err
				}
			}

			if bar != nil {
				bar.Add(len(batch))
			}
			atomic.AddInt64(&insertedCount, int64(len(batch)))
		}
		return nil
	})
}

func readFileAndExtractDecisions(f *zip.File) ([]Decision, error) {
	var localDecisions []Decision
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// nested zip
	if filepath.Ext(f.Name) == ".zip" {
		nestedData, _ := io.ReadAll(rc)
		nestedZip, err := zip.NewReader(bytes.NewReader(nestedData), int64(len(nestedData)))
		if err != nil {
			return nil, fmt.Errorf("failed to open nested zip file %s: %w", f.Name, err)
		}
		for _, nf := range nestedZip.File {
			if err := readCSV(nf, &localDecisions); err != nil {
				return nil, fmt.Errorf("failed to read CSV from nested zip file %s: %w", nf.Name, err)
			}
		}
	} else {
		if err := readCSVFile(rc, &localDecisions); err != nil {
			return nil, fmt.Errorf("failed to read CSV from file %s: %w", f.Name, err)
		}
	}

	return localDecisions, nil
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
		return fmt.Errorf("failed to read CSV file: %w", err)
	}

	headers := make(map[string]int)
	for i, h := range records[0] {
		headers[h] = i
	}

	for rowIndex, record := range records[1:] {
		decision := parseDecision(headers, record)
		*allDecisions = append(*allDecisions, decision)

		if decision.UUID == "" {
			fmt.Printf("⚠️ Warning: Missing UUID in row %d: %+v\n", rowIndex+1, record)
		}
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

	// Try JSON parse first
	var arr []string
	if err := json.Unmarshal([]byte(value), &arr); err == nil {
		return arr
	}

	// Fallback
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
