# stores shared constants for unit tests
from datetime import date

# filenames
TEST_LOG_FILEPATH = f"D:\\dev\\projects\\001_python\\005_py_etlloader\\logs\\etlloader_{date.today().isoformat()}.log"
TEST_JSON_CONFIG_FILEPATH = f"D:\\dev\\projects\\001_python\\005_py_etlloader\\data\\etltables_config.json"

# DB related
TEST_TABLENAME_ENTITY = "etl_entity"
TEST_TABLENAME_TRANSACTIONS = "etl_transactions"