import os
import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
from datetime import datetime
from influxdb import DataFrameClient

def write_to_db(payload, db_client):
	#Edits received CSV file from broker to add actual mV values
	#And writes them into InfluxDB
	print("Received Message")
	#create new file and write the received msg on it
	with open('received.csv', 'wb') as fd:
	    fd.write(payload)
	#Create dataframe
	df = pd.read_csv('received.csv')
	#Calculate actual mV measurement
	df['mV'] = df['value']*0.125
	#Delete old value of bits
	del df['value']
	#Convert the received timestamp into a pandas datetime object
	df['date_time'] = pd.to_datetime(df['time_stamp'])
	#set a DateTime index and delete the old time_stamp columns
	df = df.set_index(pd.DatetimeIndex(df['date_time']))
	del df['time_stamp'], df['date_time']
	#Seperate the dataframe by groups of adc's and channels
	#Given we are only going to be using one field ('mV')
	#Tags are given as a dict
	grouped = df.groupby(['adc','channel'])
	for group in grouped.groups:
		adc, channel = group
		tags = dict(adc=adc, channel=channel)
		sub_df = grouped.get_group(group)[['mV']]
		db_client.write_points(sub_df, 'measurements', tags=tags)
	print('Data Written to DB')
	os.remove('received.csv')
	
	
#Globals
ADC_resolution_bits = 16
ADC_resolution = (2**ADC_resolution_bits) - 1

#Function used in data conversion functions
#Linearly Maps Data from one range to another
def mapData(data, dataMIN, dataMAX, newMIN, newMAX):
	return (data - dataMIN) / (dataMAX - dataMIN) * (newMAX - newMIN) + newMIN


	

	
#---------------- MQ131 Sensor -------------------
'''
MQ131 Ozone Sensor Functions


Go from: 'Voltage --> ADC --> ppm' by using the following functions:
	MQ131_ADC_to_ppm(MQ131_voltage_to_ADC(voltageMeasured))

Go from: 'ADC --> ppm' by using the following functions:
	MQ131_ADC_to_ppm(ADC_value)
	
Go from: 'Voltage --> ppm' by using the following functions:
	MQ131_voltage_to_ppm(voltageMeasured)	
'''

#Sensor Parameters
MQ131_voltageMIN = 0
MQ131_voltageMAX = 5
MQ131_ppmMIN = .01
MQ131_ppmMAX = 2

def MQ131_voltage_to_ADC(voltageMeasured):
	#Converts the outputted sensor voltage into an ADC value based on the resolution of the ADC (defined at the top of the program)
	ADC = (ADC_resolution/MQ131_voltageMAX) * voltageMeasured
	return int(ADC)

def MQ131_ADC_to_ppm(ADC):
	#Converts ADC value (0-ADC_resolution) into ppm
	ozoneppm = mapData(ADC, 0, ADC_resolution, MQ131_ppmMIN, MQ131_ppmMAX)
	return ozoneppm
	
def MQ131_ADC_to_voltage(ADC):
	#Converts ADC value (0-ADC_resolution) back into voltage value
	#(Not a necessary function, but included just incase it becomes useful later on)
	voltage = (ADC * MQ131_voltageMAX)/ADC_resolution
	return voltage
	
def MQ131_voltage_to_ppm(voltage):
	#Converts voltage directly into ppm
	#(Not a necessary function, but included incase it becomes useful later on)
	ozoneppm = mapData(voltage, MQ131_voltageMIN, MQ131_voltageMAX, MQ131_ppmMIN, MQ131_ppmMAX)
	return ozoneppm
	
	
	
	
	
#---------------- MQ9 Sensor -------------------
'''
MQ9 Carbon Monoxide and Combustible Gas Sensor Functions


Go from: 'Voltage --> ADC --> ppm' by using the following functions:
	MQ9_ADC_to_ppm(MQ9_voltage_to_ADC(voltageMeasured))

Go from: 'ADC --> ppm' by using the following functions:
	MQ9_ADC_to_ppm(ADC_value)
	
Go from: 'Voltage --> ppm' by using the following functions:
	MQ9_voltage_to_ppm(voltageMeasured)	
'''

#Sensor Parameters
MQ9_voltageMIN = 0
MQ9_voltageMAX = 5
MQ9_ppmMIN = 10
MQ9_ppmMAX = 1000

def MQ9_voltage_to_ADC(voltageMeasured):
	#Converts the outputted sensor voltage into an ADC value based on the resolution of the ADC (defined at the top of the program)
	ADC = (ADC_resolution/MQ9_voltageMAX) * voltageMeasured
	return int(ADC)
	
def MQ9_ADC_to_ppm(ADC):
	#Converts ADC value (0-ADC_resolution) into ppm
	oxygenppm = mapData(ADC, 0, ADC_resolution, MQ9_ppmMIN, MQ9_ppmMAX)
	return oxygenppm
	
def MQ9_ADC_to_voltage(ADC):
	#Converts ADC value (0-ADC_resolution) back into voltage value
	#(Not a necessary function, but included just incase it becomes useful later on)
	voltage = (ADC * MQ9_voltageMAX)/ADC_resolution
	return voltage
	
def MQ9_voltage_to_ppm(voltage):
	#Converts voltage directly into ppm
	#(Not a necessary function, but included incase it becomes useful later on)
	oxygenppm = mapData(voltage, MQ9_voltageMIN, MQ9_voltageMAX, MQ9_ppmMIN, MQ9_ppmMAX)
	return oxygenppm
	
	
	
	
	
#---------------- Magnetometer Sensor -------------------  (Not Finished)
'''
Magnetometer Sensor Functions


Go from: 'Voltage --> ADC --> tesla' by using the following functions:
	magnetometer_ADC_to_tesla(magnetometer_voltage_to_ADC(voltageMeasured))

Go from: 'ADC --> tesla' by using the following functions:
	magnetometer_ADC_to_tesla(ADC_value)
	
Go from: 'Voltage --> tesla' by using the following functions:
	magnetometer_voltage_to_tesla(voltageMeasured)
'''

#Sensor Paramters
magnetometer_voltageMIN = 0 #----------- Not Actual Values
magnetometer_voltageMAX = 3 #----------- Not Actual Values
magnetometer_teslaMIN = 0  #------------ Not Actual Values
magnetometer_teslaMAX = 1 #------------- Not Actual Values

def magnetometer_voltage_to_ADC(voltageMeasured):
	#Converts the outputted sensor voltage into an ADC value based on the resolution of the ADC (defined at the top of the program)
	ADC = (ADC_resolution/magnetometer_voltageMAX) * voltageMeasured
	return int(ADC)

def magnetometer_ADC_to_tesla(ADC):
	#Converts ADC value (0-ADC_resolution) into teslas
	return mapData(ADC, 0, ADC_resolution, magnetometer_teslaMIN, magnetometer_teslaMAX)
	
def magnetometer_ADC_to_voltage(ADC):
	#Converts ADC value (0-ADC_resolution) back into voltage value
	#(Not a necessary function, but included just incase it becomes useful later on)
	voltage = (ADC * magnetometer_voltageMAX) / ADC_resolution
	return voltage

def magnetometer_voltage_to_tesla(voltage):
	#Converts voltage directly into teslas
	#(Not a necessary function, but included incase it becomes useful later on)
	return mapData(voltage, magnetometer_voltageMIN, magnetometer_voltageMAX, magnetometer_teslaMIN, magnetometer_teslaMAX)
	
	
	
	
	
#---------------- soilpH Sensor -------------------          (Not Finished)
'''
soilpH Sensor Functions


Go from: 'Voltage --> ADC --> pH' by using the following functions:
	soilpH_ADC_to_pH(soilpH_voltage_to_ADC(voltageMeasured))

Go from: 'ADC --> pH' by using the following functions:
	soilpH_ADC_to_pH(ADC_value)
	
Go from: 'Voltage --> pH' by using the following functions:
	soilpH_voltage_to_pH(voltageMeasured)	
'''

#Sensor Paramters
soilpH_voltageMIN = 0 #------- Not Actual Values
soilpH_voltageMAX = 3 #------- Not Actual Values
soilpH_pHMIN = 0 #------------ Not Actual Values
soilpH_pHMAX = 14 #----------- Not Actual Values

def soilpH_voltage_to_ADC(voltageMeasured):
	#Converts the outputted sensor voltage into an ADC value based on the resolution of the ADC (defined at the top of the program)
	ADC = (ADC_resolution/soilpH_voltageMAX) * voltageMeasured
	return int(ADC)

def soilpH_ADC_to_pH(ADC):
	#Converts ADC value (0-ADC_resolution) into pH
	return mapData(ADC, 0, ADC_resolution, soilpH_pHMIN, soilpH_pHMAX)

def soilpH_ADC_to_voltage(ADC):
	#Converts ADC value (0-ADC_resolution) back into voltage value
	#(Not a necessary function, but included just incase it becomes useful later on)
	voltage = (ADC * soilpH_voltageMAX) / ADC_resolution
	return voltage

def soilpH_voltage_to_pH(voltage):
	#Converts voltage directly into pH
	#(Not a necessary function, but included incase it becomes useful later on)
	return mapData(voltage, soilpH_voltageMIN, soilpH_voltageMAX, soilpH_phMIN, soilpH_pHMAX)
	
	
	
	
	
#---------------- waterpH Sensor ------------------- (Not Finished)
'''
waterpH Sensor Functions


Go from: 'Voltage --> ADC --> pH' by using the following functions:
	waterpH_ADC_to_pH(waterpH_voltage_to_ADC(voltageMeasured))

Go from: 'ADC --> pH' by using the following functions:
	waterpH_ADC_to_pH(ADC_value)
	
Go from: 'Voltage --> pH' by using the following functions:
	waterpH_voltage_to_pH(voltageMeasured)
'''

#Sensor Paramters
waterpH_voltageMIN = 0 #------- Not Actual Values
waterpH_voltageMAX = 3 #------- Not Actual Values
waterpH_pHMIN = 0 #------------ Not Actual Values
waterpH_pHMAX = 14 #----------- Not Actual Values

def waterpH_voltage_to_ADC(voltageMeasured):
	#Converts the outputted sensor voltage into an ADC value based on the resolution of the ADC (defined at the top of the program)
	ADC = (ADC_resolution/waterpH_voltageMAX) * voltageMeasured
	return int(ADC)

def waterpH_ADC_to_pH(ADC):
	#Converts ADC value (0-ADC_resolution) into pH
	return mapData(ADC, 0, ADC_resolution, waterpH_pHMIN, waterpH_pHMAX)
	
def waterpH_ADC_to_voltage(ADC):
	#Converts ADC value (0-ADC_resolution) back into voltage value
	#(Not a necessary function, but included just incase it becomes useful later on)
	voltage = (ADC * waterpH_voltageMAX) / ADC_resolution
	return voltage

def waterpH_voltage_to_pH(voltage):
	#Converts voltage directly into pH
	#(Not a necessary function, but included incase it becomes useful later on)
	return mapData(voltage, waterpH_voltageMIN, waterpH_voltageMAX, waterpH_phMIN, waterpH_pHMAX)	
	
	
	
	
	
#---------------- soil conductivity Sensor -------------------          (Not Finished)
'''
soil conductivity Sensor Functions


Go from: 'Voltage --> ADC --> conductivity' by using the following functions:
	soilConductivity_ADC_to_conductivity(soilConductivity_voltage_to_ADC(voltageMeasured))

Go from: 'ADC --> conductivity' by using the following functions:
	soilConductivity_ADC_to_conductivity(ADC_value)
	
Go from: 'Voltage --> conductivity' by using the following functions:
	soilConductivity_voltage_to_conductivity(voltageMeasured)	
'''

#Sensor Paramters
soilConductivity_voltageMIN = 0 #------------ Not Actual Values
soilConductivity_voltageMAX = 3 #------------ Not Actual Values
soilConductivity_conductivityMIN = 0 #------- Not Actual Values
soilConductivity_conductivityMAX = 1000 #---- Not Actual Values

def soilConductivity_voltage_to_ADC(voltageMeasured):
	#Converts the outputted sensor voltage into an ADC value based on the resolution of the ADC (defined at the top of the program)
	ADC = (ADC_resolution/soilConductivity_voltageMAX) * voltageMeasured
	return int(ADC)

def soilConductivity_ADC_to_conductivity(ADC):
	#Converts ADC value (0-ADC_resolution) into conductivity
	return mapData(ADC, 0, ADC_resolution, soilConductivity_conductivityMIN, soilConductivity_conductivityMAX)

def soilConductivity_ADC_to_voltage(ADC):
	#Converts ADC value (0-ADC_resolution) back into voltage value
	#(Not a necessary function, but included just incase it becomes useful later on)
	voltage = (ADC * soilConductivity_voltageMAX) / ADC_resolution
	return voltage

def soilConductivity_voltage_to_conductivity(voltage):
	#Converts voltage directly into conductivity
	#(Not a necessary function, but included incase it becomes useful later on)
	return mapData(voltage, soilConductivity_voltageMIN, soilConductivity_voltageMAX, soilConductivity_conductivityMIN, soilConductivity_conductivityMAX)
	
	
	
	
	
#---------------- water conductivity Sensor ------------------- (Not Finished)
'''
water conductivity Sensor Functions


Go from: 'Voltage --> ADC --> conductivity' by using the following functions:
	waterConductivity_ADC_to_conductivity(soilConductivity_voltage_to_ADC(voltageMeasured))

Go from: 'ADC --> conductivity' by using the following functions:
	waterConductivity_ADC_to_conductivity(ADC_value)
	
Go from: 'Voltage --> conductivity' by using the following functions:
	waterConductivity_voltage_to_conductivity(voltageMeasured)	
'''

#Sensor Paramters
waterConductivity_voltageMIN = 0 #----------------- Not Actual Values
waterConductivity_voltageMAX = 3 #----------------- Not Actual Values
waterConductivity_conductivityMIN = 0 #------------ Not Actual Values
waterConductivity_conductivityMAX = 1000 #--------- Not Actual Values

def waterConductivity_voltage_to_ADC(voltageMeasured):
	#Converts the outputted sensor voltage into an ADC value based on the resolution of the ADC (defined at the top of the program)
	ADC = (ADC_resolution/waterConductivity_voltageMAX) * voltageMeasured
	return int(ADC)

def waterConductivity_ADC_to_conductivity(ADC):
	#Converts ADC value (0-ADC_resolution) into conductivity
	return mapData(ADC, 0, ADC_resolution, waterConductivity_conductivityMIN, waterConductivity_conductivityMAX)

def waterConductivity_ADC_to_voltage(ADC):
	#Converts ADC value (0-ADC_resolution) back into voltage value
	#(Not a necessary function, but included just incase it becomes useful later on)
	voltage = (ADC * waterConductivity_voltageMAX) / ADC_resolution
	return voltage

def waterConductivity_voltage_to_conductivity(voltage):
	#Converts voltage directly into conductivity
	#(Not a necessary function, but included incase it becomes useful later on)
	return mapData(voltage, waterConductivity_voltageMIN, waterConductivity_voltageMAX, waterConductivity_conductivityMIN, waterConductivity_conductivityMAX)	
	


def main():
	"""
	MQTT Client connector in charge of receiving the 10 Hz csv files,
	perform calculations and store them in the database
	"""
	#influxdb information for connection -- right now is local	
	db_host = 'influxdb'
	db_port = 8086
	db_username = 'root'
	db_password = 'root'
	database = 'testing'
	
	#info of the MQTT broker
	host = "35.237.36.219"
	port = 1883
	keepalive = 30
	client_id = None #client_id is randomly generated
	topic = "usa/quincy/1"

	def on_connect(client, userdata, flags, rc):
		if rc == 0:
			# Callback for when the client receives a CONNACK response from the server.
			print("Connected with result code {}".format(rc))
			# Subscribes to topic with QoS 2
			client.subscribe(topic, 2)
		else:
			print("Error in connection")

	def on_message(client, userdata, msg):
		# The callback for when a PUBLISH message is received from the server.
		#Detects an arriving message (CSV) and writes it in the db
	    payload = msg.payload
	    try:
	        write_to_db(payload, db_client)
	    except: #This needs to be changed
		    print("Error")

	# connects to database and creates new database
	db_client = DataFrameClient(host=db_host, port=db_port, username=db_username, password=db_password, database=database)
	db_client.create_database('testing')

	#Establish conection with broker and start receiving messages
	# Params -> Client(client_id=””, clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)
	# We set clean_session to False, so in case connection is lost, it'll reconnect with same ID
	# For debug purposes (client_id is not defined) we'll set it to True
	client = mqtt.Client(client_id=client_id, clean_session=True)
	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(host, port, keepalive)
	
	# Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
	client.loop_forever()

if __name__ == '__main__':
	main()
