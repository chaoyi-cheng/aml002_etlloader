# test_etlloader_002_validation.py: covers validation functions of ETLLoader
import pytest
from src.etlloader import ETLLoader
import pytest_constants

@pytest.fixture(scope="module")
def fixture_etlloader():
    # setup
    etlloader = ETLLoader(pytest_constants.TEST_LOG_FILEPATH, pytest_constants.TEST_JSON_CONFIG_FILEPATH) # use the standard constructor, we want a fully initialized object

    # yield
    yield etlloader

    # teardown

def test_validate_csv_row_emptyvalues(fixture_etlloader):
    # tests that empty values are successfully detected when the configuration requires it
    test_input_row = {
        "csv_tran_id":"TRANID_TEST0001",
        "csv_entity_id":"ENTITYID_TEST0001",
        "csv_tran_type":"TRANTYPE_DEPOSIT",
        "csv_tran_amount_int": "1000",
        "csv_tran_amount_float":"1000.00",
        "csv_tran_currency":"USD",
        "csv_tran_datetime":"2026-07-27 01:30:59"
        # "csv_tran_crdb":"true", CRDB (boolean) is missing
        # "csv_tran_details":"", optional
    }

    # ensure that mappings exist
    target_mapping_config = fixture_etlloader.get_table_mapping_config(pytest_constants.TEST_TABLENAME_TRANSACTIONS)
    assert target_mapping_config
    target_column_mappings = target_mapping_config[fixture_etlloader.JSON_TAG_COLUMNMAPPINGS]
    assert target_column_mappings

    validation_result, validation_msg = (
        fixture_etlloader.validate_csv_row_against_column_mappings(test_input_row, target_column_mappings))

    # we expect validation to fail because of the missing CRDB
    assert(not validation_result)
    assert "csv_tran_crdb".lower() in validation_msg.lower()

    print("Test Case: test_validate_csv_row_emptyvalues")
    print(f"Validation Result: {validation_result}")
    print(f"Validation Message: {validation_msg}")

def test_validate_csv_row_allvalues_positive(fixture_etlloader):
    # tests the positive case for validating all types of values
    test_input_row = {
        "csv_tran_id": "TRANID_TEST0001",
        "csv_entity_id": "ENTITYID_TEST0001",
        "csv_tran_type": "TRANTYPE_DEPOSIT",
        "csv_tran_amount_int": "1000",
        "csv_tran_amount_float": "1000.00",
        "csv_tran_currency": "USD",
        "csv_tran_datetime": "2026-07-27 01:30:59",
        "csv_tran_crdb":"1",
        "csv_tran_details":""
    }

    # ensure that mappings exist
    target_mapping_config = fixture_etlloader.get_table_mapping_config(pytest_constants.TEST_TABLENAME_TRANSACTIONS)
    assert target_mapping_config
    target_column_mappings = target_mapping_config[fixture_etlloader.JSON_TAG_COLUMNMAPPINGS]
    assert target_column_mappings

    validation_result, validation_msg = (
        fixture_etlloader.validate_csv_row_against_column_mappings(test_input_row, target_column_mappings))

    # validation should pass - positive case
    assert validation_result
    assert "successful".lower() in validation_msg.lower()

    print("Test Case: test_validate_csv_row_allvalues_positive")
    print(f"Validation Result: {validation_result}")
    print(f"Validation Message: {validation_msg}")

def test_validate_csv_row_stringvalue_negative(fixture_etlloader):
    # tests the negative case for validating string values
    # 1) empty string
    test_input_row = {
        "csv_tran_id": "", # empty tran_id
        "csv_entity_id": "ENTITYID_TEST0001",
        "csv_tran_type": "TRANTYPE_DEPOSIT",
        "csv_tran_amount_int": "1000",
        "csv_tran_amount_float": "1000.00",
        "csv_tran_currency": "USD",
        "csv_tran_datetime": "2026-07-27 01:30:59",
        "csv_tran_crdb": "1",
        "csv_tran_details": ""
    }

    # ensure that mappings exist
    target_mapping_config = fixture_etlloader.get_table_mapping_config(pytest_constants.TEST_TABLENAME_TRANSACTIONS)
    assert target_mapping_config
    target_column_mappings = target_mapping_config[fixture_etlloader.JSON_TAG_COLUMNMAPPINGS]
    assert target_column_mappings

    validation_result, validation_msg = (
        fixture_etlloader.validate_csv_row_against_column_mappings(test_input_row, target_column_mappings))

    # validation should fail - negative case
    assert not validation_result
    assert "csv_tran_id".lower() in validation_msg.lower()

    print("Test Case: test_validate_csv_row_stringvalue_negative")
    print(f"Validation Result: {validation_result}")
    print(f"Validation Message: {validation_msg}")

def test_validate_csv_row_integervalue_negative(fixture_etlloader):
    # tests the negative case for validating integer values
    # 1) invalid integer value
    test_input_row = {
        "csv_tran_id": "TRANID_TEST0001",
        "csv_entity_id": "ENTITYID_TEST0001",
        "csv_tran_type": "TRANTYPE_DEPOSIT",
        "csv_tran_amount_int": "1000_INVALID", # invalid int value
        "csv_tran_amount_float": "1000.00",
        "csv_tran_currency": "USD",
        "csv_tran_datetime": "2026-07-27 01:30:59",
        "csv_tran_crdb": "1",
        "csv_tran_details": ""
    }

    # ensure that mappings exist
    target_mapping_config = fixture_etlloader.get_table_mapping_config(pytest_constants.TEST_TABLENAME_TRANSACTIONS)
    assert target_mapping_config
    target_column_mappings = target_mapping_config[fixture_etlloader.JSON_TAG_COLUMNMAPPINGS]
    assert target_column_mappings

    validation_result, validation_msg = (
        fixture_etlloader.validate_csv_row_against_column_mappings(test_input_row, target_column_mappings))

    # validation should fail - negative case
    assert not validation_result
    assert "csv_tran_amount_int".lower() in validation_msg.lower()

    print("Test Case: test_validate_csv_row_integervalue_negative")
    print(f"Validation Result: {validation_result}")
    print(f"Validation Message: {validation_msg}")

def test_validate_csv_row_floatvalue_negative(fixture_etlloader):
    # tests the negative case for validating float values
    # 1) invalid float value
    test_input_row = {
        "csv_tran_id": "TRANID_TEST0001",
        "csv_entity_id": "ENTITYID_TEST0001",
        "csv_tran_type": "TRANTYPE_DEPOSIT",
        "csv_tran_amount_int": "1000",
        "csv_tran_amount_float": "1000.00_INVALID", # invalid float value
        "csv_tran_currency": "USD",
        "csv_tran_datetime": "2026-07-27 01:30:59",
        "csv_tran_crdb": "1",
        "csv_tran_details": ""
    }

    # ensure that mappings exist
    target_mapping_config = fixture_etlloader.get_table_mapping_config(pytest_constants.TEST_TABLENAME_TRANSACTIONS)
    assert target_mapping_config
    target_column_mappings = target_mapping_config[fixture_etlloader.JSON_TAG_COLUMNMAPPINGS]
    assert target_column_mappings

    validation_result, validation_msg = (
        fixture_etlloader.validate_csv_row_against_column_mappings(test_input_row, target_column_mappings))

    # validation should fail - negative case
    assert not validation_result
    assert "csv_tran_amount_float".lower() in validation_msg.lower()

    print("Test Case: test_validate_csv_row_floatvalue_negative")
    print(f"Validation Result: {validation_result}")
    print(f"Validation Message: {validation_msg}")

def test_validate_csv_row_booleanvalue_negative(fixture_etlloader):
    # tests the negative case for validating boolean values
    # 1) invalid boolean value
    test_input_row = {
        "csv_tran_id": "TRANID_TEST0001",
        "csv_entity_id": "ENTITYID_TEST0001",
        "csv_tran_type": "TRANTYPE_DEPOSIT",
        "csv_tran_amount_int": "1000",
        "csv_tran_amount_float": "1000.00",
        "csv_tran_currency": "USD",
        "csv_tran_datetime": "2026-07-27 01:30:59",
        "csv_tran_crdb": "INVALID_BOOLEAN", # invalid boolean value
        "csv_tran_details": ""
    }

    # ensure that mappings exist
    target_mapping_config = fixture_etlloader.get_table_mapping_config(pytest_constants.TEST_TABLENAME_TRANSACTIONS)
    assert target_mapping_config
    target_column_mappings = target_mapping_config[fixture_etlloader.JSON_TAG_COLUMNMAPPINGS]
    assert target_column_mappings

    validation_result, validation_msg = (
        fixture_etlloader.validate_csv_row_against_column_mappings(test_input_row, target_column_mappings))

    # validation should fail - negative case
    assert not validation_result
    assert "csv_tran_crdb".lower() in validation_msg.lower()

    print("Test Case: test_validate_csv_row_booleanvalue_negative")
    print(f"Validation Result: {validation_result}")
    print(f"Validation Message: {validation_msg}")

def test_validate_csv_row_datetimevalue_negative(fixture_etlloader):
    # tests the negative case for validating datetime values
    # 1) invalid datetime format
    test_input_row = {
        "csv_tran_id": "TRANID_TEST0001",
        "csv_entity_id": "ENTITYID_TEST0001",
        "csv_tran_type": "TRANTYPE_DEPOSIT",
        "csv_tran_amount_int": "1000",
        "csv_tran_amount_float": "1000.00",
        "csv_tran_currency": "USD",
        "csv_tran_datetime": "2026-07-27", # invalid datetime value without timestamp
        "csv_tran_crdb": "1",
        "csv_tran_details": ""
    }

    # ensure that mappings exist
    target_mapping_config = fixture_etlloader.get_table_mapping_config(pytest_constants.TEST_TABLENAME_TRANSACTIONS)
    assert target_mapping_config
    target_column_mappings = target_mapping_config[fixture_etlloader.JSON_TAG_COLUMNMAPPINGS]
    assert target_column_mappings

    validation_result, validation_msg = (
        fixture_etlloader.validate_csv_row_against_column_mappings(test_input_row, target_column_mappings))

    # validation should fail - negative case
    assert not validation_result
    assert "csv_tran_datetime".lower() in validation_msg.lower()

    print("Test Case: test_validate_csv_row_datetimevalue_negative")
    print(f"Validation Result: {validation_result}")
    print(f"Validation Message: {validation_msg}")