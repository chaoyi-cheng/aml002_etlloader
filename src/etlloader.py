# ETL loader module which loads entities (e.g. customers) and transactions from a CSV file into the DB. Has the following features:
# 1) CSV file streaming to minimize memory use
# 2) configurable CSV to DB column mapping via an external JSON file
# 3) configurable validation for each CSV column via an external JSON file
# 4) outputs bad records to a separate, newly generated CSV file so they can be easily re-ingested
import csv
import os
from datetime import datetime
import json
import logging
from logging import Logger

from mysql.connector import Error
from connectordb import create_db_connection

# validators
class ETLValidationError(Exception):
    # custom exception class for ETL data validation errors
    pass

class ETLBaseValidator:
    # base class, all other ETL validators will be descended from this
    validator_key_str = "none" # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def validate(self, val: str) -> bool:
        return True

class ETLEmailValidator(ETLBaseValidator):
    validator_key_str = "email" # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def validate(self, val: str) -> bool:
        clean_val = str(val).strip()
        if "@" not in clean_val or "." not in clean_val:
            raise ETLValidationError(f"Invalid email format: '{val}'")
        return True

class ETLAgeRangeValidator(ETLBaseValidator):
    validator_key_str = "age_range"  # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def __init__(self, min_age: int = 0, max_age: int = 100):
        self.min_age = min_age
        self.max_age = max_age

    def validate(self, val: str) -> bool:
        try:
            age = int(val)
        except (ValueError, TypeError):
            raise ETLValidationError(f"Value must be an integer, got '{val}'")

        if not (self.min_age <= age <= self.max_age):
            raise ETLValidationError(f"Value {age} is out of bounds ({self.min_age}-{self.max_age})")
        return True

class ETLStringValidator(ETLBaseValidator):
    validator_key_str = "string"  # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def validate(self, val: str) -> bool:
        if not str(val).strip():
            raise ETLValidationError("String field cannot be empty")
        return True

class ETLIntegerValidator(ETLBaseValidator):
    validator_key_str = "integer"  # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def validate(self, val: str) -> bool:
        try:
            int(val)
            return True
        except (ValueError, TypeError):
            raise ETLValidationError(f"Value must be a valid integer, got '{val}'")

class ETLFloatValidator(ETLBaseValidator):
    validator_key_str = "float"  # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def validate(self, val: str) -> bool:
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            raise ETLValidationError(f"Value must be a valid float, got '{val}'")

class ETLBooleanValidator(ETLBaseValidator):
    validator_key_str = "boolean"  # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def validate(self, val: str) -> bool:
        if val.lower() in ['0', '1']:
            return True
        else:
            raise ETLValidationError(f"Value must be a valid boolean (0 or 1), got '{val}'")

class ETLDateTimeValidator(ETLBaseValidator):
    validator_key_str = "datetime"  # each ETLvalidator class must have a fixed validator key string, used in VALIDATOR_REGISTRY

    def __init__(self, datetime_format: str = "%Y-%m-%d %H:%M:%S"):
        self.datetime_format = datetime_format

    def validate(self, val: str) -> bool:
        try:
            datetime.strptime(str(val).strip(), self.datetime_format)
            return True
        except ValueError:
            raise ETLValidationError(f"Datetime '{val}' does not match format '{self.datetime_format}'")

VALIDATOR_REGISTRY = {
    ETLBaseValidator.validator_key_str: ETLBaseValidator(),
    ETLEmailValidator.validator_key_str: ETLEmailValidator(),
    ETLAgeRangeValidator.validator_key_str: ETLAgeRangeValidator(),
    ETLStringValidator.validator_key_str: ETLStringValidator(),
    ETLIntegerValidator.validator_key_str: ETLIntegerValidator(),
    ETLFloatValidator.validator_key_str: ETLFloatValidator(),
    ETLBooleanValidator.validator_key_str: ETLBooleanValidator(),
    ETLDateTimeValidator.validator_key_str: ETLDateTimeValidator()
}

# main ETL loader
class ETLLoader:

    # constants
    DEFAULT_LOGGER_NAME = "ETLLoader"
    DEFAULT_VALIDATOR_KEY = ETLBaseValidator.validator_key_str
    DEFAULT_ERROR_ENCODING_TYPE = "utf-8"
    DEFAULT_ERROR_VALIDATIONMSG_FIELDNAME = "validation_error"
    DEFAULT_PERSIST_BATCH_SIZE = 1000

    JSON_TAG_TABLES = "tables"
    JSON_TAG_TABLENAME = "table_name"
    JSON_TAG_COLUMNMAPPINGS = "column_mappings"
    JSON_TAG_CSVCOLUMN = "csv_column"
    JSON_TAG_DBCOLUMN = "db_column"
    JSON_TAG_PRIMARYKEYCOLUMN = "primary_key"
    JSON_TAG_CSVCOLUMN_VALIDATOR = "validator"
    JSON_TAG_CSVCOLUMN_REQUIRED = "required"

    def __init__(self, log_filepath:str, json_config_filepath:str, init_now:bool=True):
        # declares and initializes self.logfilepath and self.logger
        # we should always initialize immediately with the sole exception of test harnesses
        self.log_filepath:str = None
        self.logger:Logger = None
        if init_now:
            self.init_logging(log_filepath, self.DEFAULT_LOGGER_NAME)

        # declares and initializes self.json_config_filepath and self.table_mapping_configs
        self.json_config_filepath:str = None
        self.table_mapping_configs:list[dict] = None
        if init_now:
            self.init_json_config_file(json_config_filepath)

    def init_logging(self, log_filepath:str, logger_name:str):
        # this method can be run either during the constructor or subsequently
        self.log_filepath = log_filepath

        # clears old handlers if script is re-initialized in the same runtime session
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=log_filepath,
            filemode="a",  # use append mode so execution history persists
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=logging.INFO
        )
        self.logger = logging.getLogger(logger_name)
        self.logger.info(f"Logging engine initialized. Output targeted to: '{log_filepath}'")

    def init_json_config_file(self, json_config_filepath:str):
        # this method can be run either during the constructor or subsequently
        self.json_config_filepath = json_config_filepath

        if not os.path.exists(json_config_filepath):
            raise FileNotFoundError(f"JSON config file not found at: '{json_config_filepath}'")

        with open(json_config_filepath, "r") as json_config_file:
            table_mapping_configs:list[dict] = json.load(json_config_file)[self.JSON_TAG_TABLES]

            # validate JSON, then assign into instance variable
            self.validate_table_mapping_configs(table_mapping_configs)
            self.table_mapping_configs = table_mapping_configs
            self.logger.info(f"JSON configuration file at '{json_config_filepath}' was successfully loaded.")
            self.logger.info(f"Table mapping configs: {self.table_mapping_configs}")

    def validate_table_mapping_configs(self, table_mapping_configs:list[dict]):
        for table_mapping_config in table_mapping_configs:
            table_name = table_mapping_config[self.JSON_TAG_TABLENAME]

            # validate that for each input column, both the DB and CSV column values are defined
            column_mappings = table_mapping_config[self.JSON_TAG_COLUMNMAPPINGS]
            db_cols = [mapping[self.JSON_TAG_DBCOLUMN] for mapping in column_mappings]
            csv_cols = [mapping[self.JSON_TAG_CSVCOLUMN] for mapping in column_mappings]

            if len(db_cols) != len(csv_cols):
                raise RuntimeError(f"DB and CSV column counts don't match in mappings! Table: '{table_name}', DB col count: {len(db_cols)}, CSV col count: {len(csv_cols)}")

            for db_col_val in db_cols:
                if db_col_val is None or str(db_col_val).strip() == "":
                    raise RuntimeError(f"One or more DB column values in the mappings is empty! Table: '{table_name}'")

            for csv_col_val in csv_cols:
                if csv_col_val is None or str(csv_col_val).strip() == "":
                    raise RuntimeError(f"One or more CSV column values in the mappings is empty! Table: '{table_name}'")

            # ensure that all specified validators are of a valid type
            # construct valid list of validators
            registered_validator_keystr:list = VALIDATOR_REGISTRY.keys()

            for mapping in column_mappings:
                input_validator_val = mapping[self.JSON_TAG_CSVCOLUMN_VALIDATOR]
                if input_validator_val not in registered_validator_keystr:
                    raise RuntimeError(f"One or more validator values in the mappings is invalid! Table: '{table_name}', Input validator value: {input_validator_val}")

            # write to logger if validation completes successfully
            self.logger.info(f"JSON table mapping configuration was successfully validated.")

    def get_table_mapping_config(self, table_name:str):
        for table_mapping_config in self.table_mapping_configs:
            if table_mapping_config[self.JSON_TAG_TABLENAME] == table_name:
                return table_mapping_config
        return None

    def validate_csv_row_against_column_mappings(self, input_row:dict, target_column_mappings:list) -> tuple[bool, str]:
        # validates a single input row from the CSV
        validator_key:str = None

        for csv_col_mapping in target_column_mappings:
            csv_col = csv_col_mapping[self.JSON_TAG_CSVCOLUMN]
            csv_col_val = input_row.get(csv_col)

            # 1) if a required column is not found in the input row, or
            # 2) if a required column value is null or empty
            if csv_col not in input_row or csv_col_val is None or str(csv_col_val).strip() == "":
                if csv_col_mapping.get(self.JSON_TAG_CSVCOLUMN_REQUIRED, False):
                    return False, f"Validator used: {validator_key}. Missing required column key: '{csv_col}'"
                continue

            # returns the configured validator for the column, or the default validator if none has been configured
            validator_key = csv_col_mapping.get(self.JSON_TAG_CSVCOLUMN_VALIDATOR, self.DEFAULT_VALIDATOR_KEY)
            validator = VALIDATOR_REGISTRY.get(validator_key, VALIDATOR_REGISTRY[self.DEFAULT_VALIDATOR_KEY])

            # run validator on the actual value
            try:
                validator.validate(csv_col_val)
            except ETLValidationError as e:
                return False, f"Validator used: {validator_key}. Column '{csv_col}' failed validation: {e}"

        return True, f"Validator used: {validator_key}. Validation was successful"

    def persist_batch(self, target_table:str, target_column_mappings:list, current_batch:list[dict]) -> int:
        # this method returns the number of rows inserted
        # note: we re-create the DB connection for each batch
        rows_inserted = 0

        if not current_batch:
            return 0

        with create_db_connection() as db_connection:
            with db_connection.cursor() as db_cursor:
                try:
                    # construct SQL INSERT query string
                    db_cols = [mapping[self.JSON_TAG_DBCOLUMN] for mapping in target_column_mappings]
                    csv_cols = [mapping[self.JSON_TAG_CSVCOLUMN] for mapping in target_column_mappings]
                    primarykey_cols = []
                    update_cols = []

                    for mapping in target_column_mappings:
                        # since we are constructing SQL, append the DB column
                        if mapping.get(self.JSON_TAG_PRIMARYKEYCOLUMN, False):
                            primarykey_cols.append(mapping[self.JSON_TAG_DBCOLUMN])
                        else:
                            update_cols.append(mapping[self.JSON_TAG_DBCOLUMN])

                    sql_columns = ", ".join(db_cols)
                    sql_params = ", ".join(["%s"] * len(db_cols))
                    sql_update_columns = ", ".join([f"{col} = VALUES({col})" for col in update_cols])

                    sql_insert_query = f"""
                    INSERT INTO {target_table} ({sql_columns})
                    VALUES ({sql_params})
                    ON DUPLICATE KEY UPDATE {sql_update_columns};
                    """

                    records = []
                    for row in current_batch:
                        record_tuple = tuple(row.get(csv_col) for csv_col in csv_cols)
                        records.append(record_tuple)

                    db_cursor.executemany(sql_insert_query, records)
                    db_connection.commit()
                    rows_inserted = db_cursor.rowcount

                except Error as e:
                    self.logger.error(f"Database Batch Failure on table '{target_table}': {e}")
                    if db_connection:
                        db_connection.rollback()

        return rows_inserted

    def load_csv_file(self, target_table:str, csv_filepath:str, error_filepath:str, csv_encoding_type:str="utf-8"):
        # validate input params
        if not os.path.exists(csv_filepath):
            self.logger.error(f"Input target dataset file missing: {csv_filepath}")
            raise FileNotFoundError(f"Input target dataset file missing: {csv_filepath}")

        # init mapping configs
        target_table_mapping_config = self.get_table_mapping_config(target_table)
        if target_table_mapping_config is None:
            self.logger.error(f"Table profile '{target_table}' not defined inside mapping configuration file.")
            raise ValueError(f"Table profile '{target_table}' not defined inside mapping configuration file.")

        target_column_mappings = target_table_mapping_config[self.JSON_TAG_COLUMNMAPPINGS]

        # init counters
        total_processed = 0
        success_count = 0
        failed_count = 0
        db_persisted_total = 0
        current_batch = []

        # stream table data from CSV into memory
        self.logger.info(f"Initializing memory stream targeting table '{target_table}' using source file: {csv_filepath}")

        with open(csv_filepath, mode="r", encoding=csv_encoding_type) as csv_infile, \
                open(error_filepath, mode="w", encoding=self.DEFAULT_ERROR_ENCODING_TYPE, newline="") as error_outfile:

            csv_reader = csv.DictReader(csv_infile)
            error_writer = None

            for csv_row in csv_reader:
                total_processed += 1

                # validate each input row
                csv_row_is_valid, csv_row_error_msg = self.validate_csv_row_against_column_mappings(csv_row, target_column_mappings)

                if csv_row_is_valid:
                    # add the valid row to the current batch
                    current_batch.append(csv_row)
                    success_count += 1

                    # persist the current batch if it's full
                    if len(current_batch) >= self.DEFAULT_PERSIST_BATCH_SIZE:
                        self.logger.info(f"Current batch size of {len(current_batch)} is greater than {self.DEFAULT_PERSIST_BATCH_SIZE}, persisting batch.")
                        db_persisted_total += self.persist_batch(target_table, target_column_mappings, current_batch)
                        current_batch.clear()
                        self.logger.info(f"Batch successfully persisted and batch size reset!")
                else:
                    failed_count += 1
                    self.logger.warning(f"[{target_table.upper()}] Skipping row index {total_processed}, error message: {csv_row_error_msg}")

                    # initialize the error writer if this is the first error
                    if error_writer is None:
                        error_fieldnames = list(csv_reader.fieldnames) + [self.DEFAULT_ERROR_VALIDATIONMSG_FIELDNAME] if csv_reader.fieldnames else [self.DEFAULT_ERROR_VALIDATIONMSG_FIELDNAME]
                        error_writer = csv.DictWriter(error_outfile, fieldnames=error_fieldnames)
                        error_writer.writeheader()

                    # write the error to the output file
                    csv_row[self.DEFAULT_ERROR_VALIDATIONMSG_FIELDNAME] = csv_row_error_msg
                    error_writer.writerow(csv_row)

            # handle any leftover rows remaining in the last batch of the loop
            if current_batch:
                db_persisted_total += self.persist_batch(target_table, target_column_mappings, current_batch)
                current_batch.clear()

        # log a summary
        load_csv_summary_log = (
            f"\n==================================================\n"
            f"RUN SUMMARY FOR TARGET TABLE: {target_table.upper()}\n"
            f"==================================================\n"
            f"CSV Input File: {csv_filepath} (Encoding={csv_encoding_type.upper()})\n"
            f"Total Rows Processed: {total_processed}\n"
            f"Successful Rows:     {success_count}\n"
            f"Failed/Skipped Rows: {failed_count}\n"
            f"DB Operational Delta:{db_persisted_total}\n"
            f"Error Export Log:    {error_filepath if failed_count > 0 else 'N/A - No Errors Thrown'}\n"
            f"=================================================="
        )
        self.logger.info(load_csv_summary_log)

        # if there are no errors, remove the empty error file from the filesystem
        if failed_count == 0:
            try:
                os.remove(error_filepath)
                self.logger.info(f"No errors encountered for table '{target_table.upper()}'. The empty error file has been cleaned up.")
            except FileNotFoundError:
                self.logger.info(f"The file '{error_filepath}' does not exist.")
            except PermissionError:
                self.logger.info(f"Permission denied to delete this file '{error_filepath}'.")