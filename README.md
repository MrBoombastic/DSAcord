# DSAcord

A simple utility for downloading Discord data from
the [DSA Transparency Database](https://transparency.dsa.ec.europa.eu/explore-data/download?from_date=&amp;to_date=&amp;uuid=caca0689-3c4f-4a72-8a10-ddc719d22256)
and storing it locally in your Postgres.
Written in Go, of course.

![hero.png](docs/hero.png)
*Ugly image by ChatGPT. Thanks to [MinerPL](https://github.com/MinerPL) for inspiring me to create this tool. 😻*

## Functionality

This project is designed to download transparency data from the Digital Services Act (DSA) Transparency Database and
store it locally in a PostgreSQL database.
The tool automates the downloading of ZIP archives, extracts detailed records,
and inserts them in bulk.
You can specify the date range of the required data, and the tool will handle parallel
downloads, processing, and data insertion, while keeping track of execution time and table size.

✅ Download daily data dumps based on user-specified date ranges.
✅ Extracting nested ZIP files in parallel using goroutines and a WaitGroup.
✅ Showing a conditional progress bar only if there is a single worker.
✅ Bulk insertion into PostgreSQL with transaction handling to ensure atomicity.
✅ Displaying the total number of rows inserted, the time taken, and the size of the database table upon completion.

> [!NOTE]  
> There is no data available to download before 2024-08-21.
> Also, fresh data may be delayed.
> Watch out!

## Usage Examples

> [!WARNING]  
> Be careful with the number of workers.
> The memory usage can be very high.

> [!NOTE]  
> The database must already exist before importing.
> The table will be created automatically.

### Help

```bash
./dsacord --help
```

### Single worker (for slower CPUs/lower memory machines):

```bash
./dsacord --dbhost=localhost --dbuser=postgres --dbpassword=secret --from=2024-12-28 --to=2025-03-24 --workers=1
```

### Multiple workers (much faster):

```bash
./dsacord --dbhost=localhost --dbuser=postgres --dbpassword=secret --from=2024-12-28 --to=2025-03-24 --workers=5
```

> [!NOTE]  
> There are two recently added flags: `overwriteDuplicates` and `skipCheckingDuplicates`.
> There are actually duplicated entries in the source files,
> so the first flag is recommended to use if you don't care about single entries being overwritten.
> The latter one is experimental and may increase or decrease insert time in various scenarios - test it yourself.

## Database notes

The data is stored in a table called `decisions` with a schema that matches the one in the CSV files.
However, for clarity, PlatformUID is split into SnowflakeTime, EntityID and EntityType.
The table is created automatically if it does not exist, but the selected database IS NOT.
The table will follow the rules of [automigration by Gorm](https://gorm.io/docs/migration.html) along with all the
nuances.

## Test

```bash
./dsacord --dbuser postgres --dbpassword root --from=2024-12-28 --to=2025-08-08 --workers=5 --overwriteDuplicates --skipCheckingDuplicates
ℹ️  DSAcord v0.2.0
✅  Connected to the database
📆  Importing from 2024-12-28 to 2025-08-08
⚠️  Your --to date is in the future or in today. This may result in excess 404 errors.
💾  Inserting decisions in parallel. Progress bar will not be shown.
💀  Watch out: duplicated keys will be silently overwritten!
2025/08/07 22:43:51 Start!

(cut...)

2025/08/07 22:49:54 🌍  Downloading https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2025-08-08-full.zip
2025/08/07 22:49:54 Error: download failed for https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2025-08-07-full.zip: forbidden or does not exist
2025/08/07 22:49:54 Error: download failed for https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2025-08-08-full.zip: forbidden or does not exist

✅  Rows inserted: 14405318
⏱  Elapsed time: 6m19.644562s
📁  Table size: 15 GB
```
