package main

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB
var numWorkers int
var insertedCount int64

func connectDB(cfg *Config) *gorm.DB {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName,
	)

	d, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	return d
}

func main() {
	startTime := time.Now()
	app := &cli.App{
		Name:  "dsacord",
		Usage: "Simple utility to download Discord data from DSA Transparency Database",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "dbhost", EnvVars: []string{"DB_HOST"}, Value: "localhost", Usage: "Database host", Required: true},
			&cli.IntFlag{Name: "dbport", EnvVars: []string{"DB_PORT"}, Value: 5432, Usage: "Database port"},
			&cli.StringFlag{Name: "dbuser", EnvVars: []string{"DB_USER"}, Usage: "Database user", Required: true},
			&cli.StringFlag{Name: "dbpassword", EnvVars: []string{"DB_PASSWORD"}, Usage: "Database password", Required: true},
			&cli.StringFlag{Name: "dbname", EnvVars: []string{"DB_NAME"}, Value: "dsacord", Usage: "Database name", Required: true},
			&cli.StringFlag{Name: "from", Usage: "Start date (format: YYYY-MM-DD)", Required: true},
			&cli.StringFlag{Name: "to", Usage: "End date (format: YYYY-MM-DD)", Required: true},
			&cli.IntFlag{Name: "workers", Value: 1, Usage: "Number of workers for downloading and processing data, max of 5 is recommended, disables progressbar"},
		},
		Action: func(c *cli.Context) error {
			buildInfo, ok := debug.ReadBuildInfo()
			if !ok {
				return fmt.Errorf("failed to read build info")
			}
			fmt.Println("ℹ️ DSAcord " + buildInfo.Main.Version)

			var cfg Config
			fromStr := c.String("from")
			toStr := c.String("to")

			from, err := time.Parse("2006-01-02", fromStr)
			if err != nil {
				return fmt.Errorf("invalid --from date: %w", err)
			}
			to, err := time.Parse("2006-01-02", toStr)
			if err != nil {
				return fmt.Errorf("invalid --to date: %w", err)
			}
			if to.Before(from) {
				return fmt.Errorf("--to date must be after --from date")
			}

			cfg.DBHost = c.String("dbhost")
			cfg.DBPort = c.Int("dbport")
			cfg.DBUser = c.String("dbuser")
			cfg.DBPassword = c.String("dbpassword")
			cfg.DBName = c.String("dbname")
			cfg.FromDate = from
			cfg.ToDate = to
			cfg.Workers = c.Int("workers")
			numWorkers = cfg.Workers

			db = connectDB(&cfg)

			if err := db.AutoMigrate(&Decision{}); err != nil {
				panic(fmt.Sprintf("failed to migrate database: %v", err))
			}

			fmt.Println("✅  Connected to database")
			fmt.Println("📆 Importing from", cfg.FromDate.Format("2006-01-02"), "to", cfg.ToDate.Format("2006-01-02"))

			startDateWarn, _ := time.Parse("2006-01-02", "2024-08-21") // the beginning of the data dump

			if cfg.FromDate.Before(startDateWarn) {
				fmt.Println("⚠️ Your --from date is before the start of the DSA Transparency Database. It's pointless and may result in excess 404 errors.")
				time.Sleep(3 * time.Second)
			}
			// after or today
			if cfg.ToDate.After(time.Now()) || (cfg.ToDate.Format(time.DateOnly) == time.Now().Format(time.DateOnly)) {
				fmt.Println("⚠️ Your --to date is in the future or in today. This may result in excess 404 errors.")
				time.Sleep(3 * time.Second)
			}

			urls := make(chan string)
			results := make(chan error)
			done := make(chan bool)

			var wg sync.WaitGroup
			wg.Add(cfg.Workers)

			if cfg.Workers > 1 {
				log.Println("💾 Inserting decisions in parallel. Progress bar will not be shown.")
				time.Sleep(3 * time.Second)
			}

			for i := 0; i < cfg.Workers; i++ {
				go worker(urls, results, &wg)
			}

			go func() {
				for err := range results {
					if err != nil {
						log.Println("Error:", err)
					}
				}
				done <- true
			}()

			go func() {
				for d := cfg.FromDate; !d.After(cfg.ToDate); d = d.AddDate(0, 0, 1) {
					url := fmt.Sprintf("https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-%d-%02d-%02d-full.zip", d.Year(), d.Month(), d.Day())
					urls <- url
				}
				close(urls)
				wg.Wait()
				close(results)
			}()

			<-done
			elapsed := time.Since(startTime)

			fmt.Printf("\n✅  Rows inserted: %d\n", insertedCount)
			fmt.Printf("⏱  Elapsed time: %s\n", elapsed)

			var tableSizeBytes int64
			if err := db.Raw("SELECT pg_total_relation_size('decisions')").Scan(&tableSizeBytes).Error; err == nil {
				fmt.Printf("📁 'Decisions' table size: %d MB\n", tableSizeBytes/1024/1024)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

func worker(urls <-chan string, results chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	for url := range urls {
		fmt.Println("\n🌍 Downloading", url)
		data, err := downloadWithBackoff(url)
		if err != nil {
			results <- fmt.Errorf("download failed for %s: %w", url, err)
			continue
		}

		if err := processZip(data); err != nil {
			results <- fmt.Errorf("processing failed for %s: %w", url, err)
			continue
		}

		results <- nil
	}
}
