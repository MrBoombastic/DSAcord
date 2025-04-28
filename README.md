# DSAcord

Simple utility to download Discord data
from [DSA Transparency Database](https://transparency.dsa.ec.europa.eu/explore-data/download?from_date=&amp;to_date=&amp;uuid=caca0689-3c4f-4a72-8a10-ddc719d22256)
and store them locally in Postgres.
Written in Go, of course.

![hero.png](docs/hero.png)
*Ugly image by ChatGPT. Thanks to [MinerPL](https://github.com/MinerPL) for inspiring me to create this tool. 😻*

## Functionality

This project is designed to download transparency data from the DSA (Digital Services Act) Transparency Database and
store it locally in a PostgreSQL database. It automates downloading ZIP archives, extracts detailed records, and inserts
them in bulk. You can specify the date range for the data you want, and the tool handles parallel downloads, processing,
and data insertion while keeping track of execution time and table size.

✅ Downloading daily data dumps based on user-specified date ranges.  
✅ Extracting nested ZIP files in parallel using goroutines and a WaitGroup.  
✅ Conditional progress bar (shown only if there's a single worker).  
✅ Bulk insertion into PostgreSQL, with transaction handling to ensure atomicity.  
✅ Display of total inserted rows counts, the time taken, and the database table size upon completion.

> [!NOTE]  
> There is no data available to download before 2024-08-21.
> Also fresh data may be delayed. Watch out!

## Usage Examples

> [!WARNING]  
> Be careful with worker size. The memory usage can go very high.

> [!NOTE]  
> The database must exist before the import. Table will be created automatically.

### Help

```bash
./dsacord --help
```

### One worker (for slower CPUs/lesser memory machines):

```bash
./dsacord --dbhost=localhost --dbuser=postgres --dbpassword=secret --dbname=dsacord --from=2024-12-28 --to=2025-03-24 --workers=1
```

### Multiple workers (much faster):

```bash
./dsacord --dbhost=localhost --dbuser=postgres --dbpassword=secret --dbname=dsacord --from=2024-12-28 --to=2025-03-24 --workers=5
```

## Postgres

The data are stored in a table called `decisions` with the schema matching the one in CSV files, although PlatformUID is
split to SnowflakeTime, EntityID, and EntityType for clarity. The table is created automatically if it doesn't exist,
but the selected database IS NOT. The table will follow the rules
of [automigration by Gorm](https://gorm.io/docs/migration.html), with all the nuances
connected with that.

## Test

```bash
./dsacord --dbhost localhost --dbuser postgres --dbpassword root --dbname dsacord --from 2024-08-21 --to 2025-04-29 --workers 5      
✅  Connected to database
📆 Importing from 2024-08-21 to 2025-04-29
⚠️ Your --to date is in the future or in today. This may result in excess 404 errors.
2025/04/29 05:07:29 💾 Inserting decisions in parallel. Progress bar will not be shown.

🌍 Downloading https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2024-08-21-full.zip

🌍 Downloading https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2024-08-25-full.zip

(cut...)

🌍 Downloading https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2025-04-29-full.zip
2025/04/29 05:11:43 Error: download failed for https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2025-04-28-full.zip: forbidden or not exists
2025/04/29 05:11:43 Error: download failed for https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-discord-netherlands-bv-2025-04-29-full.zip: forbidden or not exists

✅  Rows inserted: 13449081
⏱  Elapsed time: 4m20.0347637s
📁 'Decisions' table size: 13592 MB
```
