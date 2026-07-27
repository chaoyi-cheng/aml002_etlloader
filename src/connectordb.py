# utility file to centralize connection creation
import mysql.connector

# static instance variables as these may need to be accessed by external classes
CONNECTOR_DB_USER = "dev"
CONNECTOR_DB_PASSWORD = "dev"
CONNECTOR_DB_HOST = "localhost"  # Use "localhost" or your server IP
CONNECTOR_DB_PORT = "3306"  # Default MySQL port
CONNECTOR_DB_DATABASE = "aml"

def create_db_connection():
    return mysql.connector.connect(
        host=CONNECTOR_DB_HOST,
        user=CONNECTOR_DB_USER,
        password=CONNECTOR_DB_PASSWORD,
        database=CONNECTOR_DB_DATABASE
    )