import json 
import requests
import json
import datetime
import numpy as np
import datetime

class fwModelV2:
	
	def __init__(self):
		self.api = "https://developers.flowworks.com/fwapi/v2/"

	def setCreds(self,userName,pwd):
		self.userName = userName
		self.pwd = pwd
		self.creds = {'UserName':self.userName,'Password':self.pwd}

	# Grab JWT token
	def getToken(self):
		# creds = {'UserName':self.userName,'Password':self.pwd}
		response = requests.post(self.api+"tokens", data=self.creds)
		key = response.json()
		self.authKey = "Bearer " + key
		
	def getAllSites(self):
		allStations = requests.get(self.api+"sites",headers={'Authorization':self.authKey}).json()
		self.allStations = json.loads(json.dumps(allStations))

	def getSiteChannels(self,idStation):
		self.stationChannels = requests.get(self.api+"sites/"+str(idStation)+"/channels",headers={'Authorization': self.authKey}).json()
		self.idStation = int(idStation)

	def getData(self,startDate,endDate,idChannel):
		# Set flag for while loop
		# Check that start and end dates are either datetime or dates
		if not isinstance(startDate,datetime.datetime):
			print("startDate is not a datetime or date object. Please try again.")
		if not isinstance(endDate,datetime.datetime):
			print("endDate is not a datetime or date object. Please try again.")
		fwData = requests.get(self.api+"sites/"+str(self.idStation)+"/channels/"+str(idChannel)+"/data",
							  headers={'Authorization': self.authKey},
							  params={'startDateFilter':startDate,'endDateFilter':endDate}).json()
		# Check code in received data
		if fwData['ResultCode'] == 0:
			# The data was retrieved successfully
			self.channelData = fwData['Resources']

		elif fwData['ResultCode'] == 1:
			# There was an error retrieving the data
			pass

		elif fwData['ResultCode'] == 2:
			# Limit exceeded
			dataInt = 5
			# Add latest data to model
			self.channelData = fwData['Resources']
			fwFlag = True
			while fwFlag:
				# Need to grab last item
				startDate = datetime.datetime.strptime(self.channelData[-1]['DataTime'],"%Y-%m-%dT%H:%M:%S")
				startDate = startDate.replace(minute=startDate.minute+dataInt)
				# Run query again until 
				fwDataChunk = requests.get(self.api+"sites/"+str(self.idStation)+"/channels/"+str(idChannel)+"/data",
								  headers={'Authorization': self.authKey},
								  params={'startDateFilter':startDate,'endDateFilter':endDate}).json()
				# Append new data
				self.channelData = self.channelData + fwDataChunk['Resources']
				if fwDataChunk['ResultCode'] == 0:
					fwFlag = False
		elif fwData['ResultCode'] == 3:
			# Invalid arguments
			print("Invalid arguments.")

		elif fwData['ResultCode'] == 4:
			# Not found
			print("Results not found.")
		else:
			# Not authorized
			print("You do not have proper authorization.")

class fwModelV1:
	
	def __init__(self):
		self.api = "https://developers.flowworks.com/fwapi/v1/"

	def setCreds(self,key):
		self.key = key
		
	def getAllSites(self):
		allStations = requests.get(self.api+self.key+"/sites").json()
		self.allStations = json.loads(json.dumps(allStations))

	def getSiteChannels(self,idStation):
		self.stationChannels = requests.get(self.api+self.key+"/site/"+str(idStation)+"/channel").json()
		self.idStation = int(idStation)

	def getData(self,startDate,endDate,idChannel):
		# Set flag for while loop
		# Check that start and end dates are strings
		if not isinstance(startDate,str):
			startDate = startDate.strftime("%Y%m%d%H%M%S")
		if not isinstance(endDate,str):
			endDate = endDate.strftime("%Y%m%d%H%M%S")
		fwData = requests.get(self.api+self.key+"/site/"+str(self.idStation)+"/channel/"+str(idChannel)+"/Data"+"/startdate/"+startDate+"/enddate/"+endDate).json()

		# Check code in received data
		if fwData['requeststatus'] == 1:
			# The data was retrieved successfully
			self.channelData = fwData['datapoints']
		else:
			# Not authorized
			print("You do not have proper authorization.")