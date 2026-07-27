# test_etlloader_003_main.py: covers all other functions of ETLLoader
import pytest
from src.etlloader import ETLLoader
import pytest_constants
from src.connectordb import create_db_connection

@pytest.fixture(scope="module")
def fixture_etlloader():
    # setup
    etlloader = ETLLoader(pytest_constants.TEST_LOG_FILEPATH, pytest_constants.TEST_JSON_CONFIG_FILEPATH) # use the standard constructor, we want a fully initialized object

    # yield
    yield etlloader

    # teardown

def test_load_csv_file_etlentity(fixture_etlloader):
    # load ETL_ENTITY DB table from CSV
    fixture_etlloader.load_csv_file(
        "etl_entity",
        f"D:\\dev\\projects\\001_python\\005_py_etlloader\\data\\etl_entity.csv",
        f"D:\\dev\\projects\\001_python\\005_py_etlloader\\data\\etl_entity_error.csv"
    )

    with create_db_connection() as db_connection:
        assert(db_connection.is_connected())
        with db_connection.cursor() as db_cursor:
            # query DB table and ensure that records are present
            sql_query = "SELECT count(0) FROM etl_entity"
            db_cursor.execute(sql_query)
            db_rows = db_cursor.fetchone()
            assert (db_rows is not None)
            assert (db_rows[0] == 3)

def test_load_csv_file_etltransactions(fixture_etlloader):
    # load ETL_TRANSACTIONS DB table from CSV
    fixture_etlloader.load_csv_file(
        "etl_transactions",
        f"D:\\dev\\projects\\001_python\\005_py_etlloader\\data\\etl_transactions.csv",
        f"D:\\dev\\projects\\001_python\\005_py_etlloader\\data\\etl_transactions_error.csv"
    )

    with create_db_connection() as db_connection:
        assert (db_connection.is_connected())
        with db_connection.cursor() as db_cursor:
            # query DB table and ensure that records are present
            sql_query = "SELECT count(0) FROM etl_transactions"
            db_cursor.execute(sql_query)
            db_rows = db_cursor.fetchone()
            assert (db_rows is not None)
            assert (db_rows[0] == 3)

def test_teardown_db_records(fixture_etlloader):
    # delete all data from DB
    # due to foreign keys, DB tables need to be cleared in this order ETL_ENTITY DB table from CSV
    with create_db_connection() as db_connection:
        assert(db_connection.is_connected())
        with db_connection.cursor() as db_cursor:
            # clear DB tables
            sql_query = "DELETE FROM etl_transactions"
            db_cursor.execute(sql_query)
            sql_query = "DELETE FROM etl_entity"
            db_cursor.execute(sql_query)
            db_connection.commit()