# test_etlloader_001_init.py: covers the init functions of ETLLoader
import pytest
from src.etlloader import ETLLoader
import pytest_constants

@pytest.fixture(scope="module")
def fixture_etlloader():
    # setup
    etlloader = ETLLoader(pytest_constants.TEST_LOG_FILEPATH, pytest_constants.TEST_JSON_CONFIG_FILEPATH, False) # we deliberately use init_now=False so we can test log init and JSON init features

    # yield
    yield etlloader

    # teardown

def test_init_logging(fixture_etlloader):
    fixture_etlloader.init_logging(pytest_constants.TEST_LOG_FILEPATH, ETLLoader.DEFAULT_LOGGER_NAME)

    # read the last line of the log and
    last_line:str = None
    with open(pytest_constants.TEST_LOG_FILEPATH, 'r', encoding='utf-8') as log_file:
        for current_line in log_file:
            last_line = current_line

    # ensure that successful logging occurred
    assert last_line.lower().find("logging engine initialized") > -1
    # ensure that the log filepath is present - this would have been written as part of successful initialization
    assert pytest_constants.TEST_LOG_FILEPATH.lower() in last_line.lower()

def test_init_json_config_file(fixture_etlloader):
    # init logging first as it is used in the JSON function
    fixture_etlloader.init_logging(pytest_constants.TEST_LOG_FILEPATH, ETLLoader.DEFAULT_LOGGER_NAME)
    # init JSON config file
    fixture_etlloader.init_json_config_file(pytest_constants.TEST_JSON_CONFIG_FILEPATH)
    # no need for additional asserts as validate_table_mapping_configs() will have raised any errors found
    assert True