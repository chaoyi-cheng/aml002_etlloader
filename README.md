# ETLLoader

ETL loader for entities (e.g. customers) and transactions from CSV file to DB. Features: 1) CSV file streaming to minimize memory use, 2) configurable CSV to DB column mappings via an external JSON file, 3) configurable validation for each CSV column, 4) outputs bad records separately so they can be easily re-ingested.

All these are standard features in most commercial ETL data loaders.

- Set up the database scripts in /db first
- Test data is in /data
- Run the pytest files in /test directory to see it in action!