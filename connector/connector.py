"""
This program runs on the server side and subscribes to a specific topic in
order to receive information from a specific publisher (Raspberry Pi)

The devices' GAIN was chosen to be 1. Since this is a 16 bits device, the
measured voltage will depend on the programmable GAIN. The following table
shows the possible reading range per chosen GAIN. A GAIN of 1 goes from -4.096V
to 4.096V.
- 2/3 = +/-6.144V
-   1 = +/-4.096V
-   2 = +/-2.048V
-   4 = +/-1.024V
-   8 = +/-0.512V
-  16 = +/-0.256V

This means that the maximum range of this 16 bits device is +/-32767.
Thus, to convert bits to V, we divide 4.096 by 32767,
which gives us 0.000125 (volts per bit). In conclusion, to convert this
readings to mV, we just need to multiply the output times 0.125,
which is done here, in the server side, to prevent time delays.
"""
import os
import json

import pandas as pd
import paho.mqtt.client as mqtt
from influxdb import DataFrameClient

DB_HOST = os.getenv("DB_HOST", "infsluxdb")
DB_PORT = int(os.getenv("DB_PORT", "8086"))
DB_USERNAME = os.getenv("DB_USER", "root")
DB_NAME = os.getenv("DB_NAME", "raw_sensor_data")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
COMMS_TOPIC = "communication/influxdbUpdate"

# MQTT broker info
BROKER_HOST = 'mqtt.eclipse.org'
BROKER_PORT = 1883
BROKER_KEEPALIVE = 30
BROKER_CLIENT_ID = None  # client_id is randomly generated
MQTT_TOPIC_LOCATIONS = [
    "usa/quincy/1",
    "usa/quincy/2",
    "test_env/usa/quincy/1"
]


def wait_for_influxdb(db_client):
    """Function to wait for the influxdb service to be available"""
    db_client.ping()


def write_to_db(payload, db_client):
    """Recieves CSV file and converts to mV values and writes to InfluxDB"""
    with open('received.csv', 'wb') as r_data:
        r_data.write(payload)

    dataframe = pd.read_csv('received.csv')
    # Convert from bits to mV
    dataframe['mV'] = dataframe['value']*0.125
    # Delete old value of bits
    del dataframe['value']
    # Convert the received timestamp into a pandas datetime object
    dataframe['date_time'] = pd.to_datetime(dataframe['time_stamp'])
    # set a DateTime index and delete the old time_stamp columns
    dataframe = dataframe.set_index(pd.DatetimeIndex(dataframe['date_time']))
    del dataframe['time_stamp'], dataframe['date_time']

    # Seperate the dataframe by groups of adc's and channels
    # Given we are only going to be using one field ('mV')
    # Tags are given as a dict
    grouped = dataframe.groupby(['adc', 'channel'])

    # Array of dictionaries that stores data on how much data was gathered in
    # each ADC/Channel. This will allow for variable amounts of data to be
    # recieved and processed correctly.
    data_points_entered = []

    for group in grouped.groups:
        adc, channel = group
        tags = dict(adc=adc, channel=channel)
        sub_df = grouped.get_group(group)[['mV']]

        # Add dictionary to array that stores information on which adc, channel
        # and how much data was published to the database with those tags
        data_points_entered.append(dict(
            adc=adc,
            channel=channel,
            amountOfData=len(sub_df)
        ))

        db_client.write_points(sub_df, 'measurements', tags=tags)

    print('Data Written to DB')
    os.remove('received.csv')
    return data_points_entered


def main():
    """
    MQTT Client connector in charge of receiving the 10 Hz csv files,
    perform calculations and store them in the database
    """

    def on_connect(client, userdata, flags, r_code):
        """Callback for when the client connects to the MQTT broker."""
        if r_code == 0:
            print("Connected with result code {}".format(r_code))
            for topic in MQTT_TOPIC_LOCATIONS:
                client.subscribe(topic, 2)
        else:
            print("Error in connection")
            raise Exception

    def on_message(client, userdata, msg):
        """The callback for when a PUBLISH message is received from the server.

        Detects an arriving message (CSV) and writes it in the db. Then, it
        forwards the calculated data for other services to make use of it.
        """
        payload = msg.payload
        data_entered_array = write_to_db(payload, db_client)
        # Adding the location of the publisher to the information that will be
        # sent to calculator
        location_and_data_array = [msg.topic, data_entered_array]
        # Publishing index information on new data added to Influx to
        # the calculator microservice
        client.publish(COMMS_TOPIC, json.dumps(location_and_data_array))

    def on_publish(*args):
        # Function for clients's specific callback when pubslishing message
        print("Comms Data Sent")

    db_client = DataFrameClient(
        host=DB_HOST,
        port=DB_PORT,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    wait_for_influxdb(db_client=db_client)
    db_client.create_database(DB_NAME)

    client = mqtt.Client(client_id=BROKER_CLIENT_ID, clean_session=True)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    client.connect(BROKER_HOST, BROKER_PORT, BROKER_KEEPALIVE)
    client.loop_forever()


if __name__ == '__main__':
    main()
