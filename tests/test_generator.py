"""CI Tests."""
import pandas as pd
from connector.connector import (
    connect_to_db,
    connect_to_mqtt_broker,
    write_to_db,
    MV_PER_BIT,
    DB_CONNECTOR_TABLE
)


def test_connect_to_db():
    """Test the system is able to connect to an infuxdb instance."""
    db_client = connect_to_db()
    assert db_client is not None
    db_client.close()


def test_connect_to_broker():
    """Test connection to our broker."""
    db_client = connect_to_db()
    mqtt_client = connect_to_mqtt_broker(db_client)
    assert mqtt_client is not None
    mqtt_client.disconnect()


def test_write_to_db():
    """Test the write to DB function with a mock CSV file."""
    db_client = connect_to_db()

    # 1. Send a mock reading file
    mock_readings_file = open("tests/ten_hz.csv")
    payload = mock_readings_file.read()
    payload = str.encode(payload)
    write_to_db(payload=payload, db_client=db_client)

    # 2. Test the values are converted correctly
    written = db_client.query('SELECT * FROM "{}"'.format(DB_CONNECTOR_TABLE))
    dataframe = written[DB_CONNECTOR_TABLE]
    raw_dataframe = pd.read_csv("tests/ten_hz.csv")
    raw_dataframe['expected_mV'] = raw_dataframe['value']*MV_PER_BIT

    print(dataframe, type(dataframe))
    print('\n\n\n', raw_dataframe)

    assert dataframe['mV'].isin(raw_dataframe['expected_mV']).all()
