package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/caarlos0/env/v10"
	"github.com/urfave/cli/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB

func connectDB(cfg *Config) *gorm.DB {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	return db
}

func main() {
	app := &cli.App{
		Name:  "dsacord",
		Usage: "Import DSA Transparency Database ",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "dbhost", EnvVars: []string{"DB_HOST"}, Value: "localhost", Usage: "Database host", Required: true},
			&cli.IntFlag{Name: "dbport", EnvVars: []string{"DB_PORT"}, Value: 5432, Usage: "Database port"},
			&cli.StringFlag{Name: "dbuser", EnvVars: []string{"DB_USER"}, Usage: "Database user", Required: true},
			&cli.StringFlag{Name: "dbpassword", EnvVars: []string{"DB_PASSWORD"}, Usage: "Database password", Required: true},
			&cli.StringFlag{Name: "dbname", EnvVars: []string{"DB_NAME"}, Value: "dsacord", Usage: "Database name", Required: true},
			&cli.StringFlag{Name: "from", Usage: "Start date (format: YYYY-MM-DD)", Required: true},
			&cli.StringFlag{Name: "to", Usage: "End date (format: YYYY-MM-DD)", Required: true},
		},
		Action: func(c *cli.Context) error {
			var cfg Config
			if err := env.Parse(&cfg); err != nil {
				return fmt.Errorf("failed to parse env vars: %w", err)
			}

			fromStr := c.String("from")
			toStr := c.String("to")

			if fromStr == "" || toStr == "" {
				return fmt.Errorf("--from and --to are required")
			}

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

			db = connectDB(&cfg)

			fmt.Println("✅  Connected to database")
			fmt.Println("📆 Importing from", cfg.FromDate.Format("2006-01-02"), "to", cfg.ToDate.Format("2006-01-02"))

			// Pass to your main processing function
			if err := db.AutoMigrate(&Decision{}); err != nil {
				panic(fmt.Sprintf("failed to migrate database: %v", err))
			}

			startDateWarn, _ := time.Parse("2006-01-02", "2024-08-21") // the beginning of the data dump

			if cfg.FromDate.Before(startDateWarn) {
				fmt.Println("⚠️ Your --from date is before the start of the DSA Transparency Database. It's pointless and may result in excess 404 errors.")
			}
			// after or today
			if cfg.ToDate.After(time.Now()) || (cfg.ToDate.Format(time.DateOnly) == time.Now().Format(time.DateOnly)) {
				fmt.Println("⚠️ Your --to date is in the future or in today. This may result in excess 404 errors.")
			}

			for d := cfg.FromDate; !d.After(cfg.ToDate); d = d.AddDate(0, 0, 1) {
				url := fmt.Sprintf("https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-%d-%02d-%02d-full.zip", d.Year(), d.Month(), d.Day())
				fmt.Println("\nDownloading", url)
				data, err := downloadWithBackoff(url)
				if err != nil {
					fmt.Println("Download failed:", err)
					continue
				}

				if err := processZip(data); err != nil {
					fmt.Println("Processing failed:", err)
					continue
				}
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}
