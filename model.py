# Import libraries
from fwApi import fwModelV1,fwModelV2
from keys import *
import datetime
import pandas as pd
import numpy as np
import sys
import time

class etModel:
	def __init__(self):
		self.fwData = {}

	def getFWData(self,startTimestamp,endTimestamp,stationId,channelIds,apiVersion="v2",days=30):
		# Set updated startTimestamp
		updatedStart = startTimestamp - datetime.timedelta(days=days)
		# Check api versin
		if (apiVersion == "v2") or (apiVersion =="V2"):
			# Initiate FlowWorks model
			fw = fwModelV2()
			fw.setCreds(userName=userName,pwd=pwd)
			fw.getToken()
		elif (apiVersion == "v1") or (apiVersion =="V1"):
			# Initiate FlowWorks model
			fw = fwModelV1()
			#fw.setCreds(userName=userName,pwd=pwd)
			fw.setCreds(key=apiKey)
		else:
			sys.exit("Please select either v1 or v2 for the FlowWorks API.")
		fw.getSiteChannels(idStation=stationId)
		# Store all requested data in channel
		for idx,ids in enumerate(channelIds):
			fw.getData(startDate=updatedStart,endDate=endTimestamp,idChannel=channelIds[idx])
			if not fw.channelData:
				sys.exit("FlowWorks returned an empty list. Please check the start and end dates and make sure they are appropriate. You may also want to check that the channel has data.")
			else:
				self.fwData[str(ids)] = fw.channelData
		# Since FlowWorks api v1 and v2 have different dictionary keys, let's place the keys in v1 to match those of v2
		if (apiVersion == "v1") or (apiVersion =="V1"):
			for key in self.fwData:
				for row in self.fwData[key]:
					# row['DataValue'] = row.pop['value']
					row['DataValue'] = row['value']
					del row['value']
					row['DataTime'] = row['date']
					del row['date']
		# Need to check number of missing days from endTimestamp to last date in each channel
		dayList = []
		#print(self.fwData)
		for key in self.fwData:
			dayList.append((endTimestamp - datetime.datetime.strptime(self.fwData[key][-1]['DataTime'],"%Y-%m-%dT%H:%M:%S")).days)
		# Loop through every item in dayList that is not zero
		for itr,vals in enumerate(dayList):
			if vals > 0:
				lastValue = self.fwData[str(channelIds[itr])][-1]['DataValue']
				lastTime = self.fwData[str(channelIds[itr])][-1]['DataTime']
				lastTimeObj = datetime.datetime.strptime(lastTime,"%Y-%m-%dT%H:%M:%S")
				for day in range(0,vals):
					# Add day to time obj
					newTimeObj = lastTimeObj + datetime.timedelta(days=1)
					# Convert back to string 
					newTimeStr = newTimeObj.strftime("%Y-%m-%dT%H:%M:%S")
					# Create row
					rowAppend = {'DataValue': str(lastValue), 'DataTime': newTimeStr}
					self.fwData[str(channelIds[itr])].append(rowAppend)
					# Update lastTimeObj
					lastTimeObj = newTimeObj
		# Check length of items. If they are all the same, then all is good, otherwise, there is missing data elsewhere in the timeseries
		expDays = (endTimestamp - updatedStart).days + 1
		updatedList = []
		#print(self.fwData[str(channelIds[itr])])
		for itr,key in enumerate(self.fwData):
			# Calculat new length
			newLength = len(self.fwData[key])
			if newLength != expDays:
				updatedList.append(itr)
		# Grab data and make pandas list
		for val in updatedList:
			dataDate = []
			dataValue = []
			dictTemp = {}
			for row in self.fwData[str(channelIds[val])]:
				dataValue.append(float(row['DataValue']))
				dataDate.append(row['DataTime'])
			dictTemp['DataTime'] = dataDate
			dictTemp['DataValue'] = dataValue
			# Convert to pandas dataframe
			dfTemp = pd.DataFrame(dictTemp)
			# Convert to datetime objects
			dfTemp['DataTime'] = pd.to_datetime(dfTemp['DataTime'],format="%Y-%m-%dT%H:%M:%S")
			# Set timestamp to index
			dfTemp.index = dfTemp['DataTime']
			# Resample dataframe every day and forward fill with previous value
			dfTemp = dfTemp.resample('86400S').ffill()
			dfTemp['DataTime'] = dfTemp.index
			# Reset index
			dfTemp.reset_index(drop=True, inplace=True)
			# Grab columns and make them into lists
			dataDateList = dfTemp['DataTime'].tolist()
			dataDate = []
			for date in dataDateList:
				dataDate.append(date.strftime("%Y-%m-%dT%H:%M:%S"))
			# Convert dataTime to string
			dataValue = dfTemp['DataValue'].tolist()
			# Convert back to dictionary
			newChannel = []
			for row in range(0,len(dataDate)):
				# Format data to FlowWorks standard
				rowItem = {'DataValue': str(dataValue[row]) , 'DataTime': str(dataDate[row])}
				newChannel.append(rowItem)
			# Replace old FlowWorks channel with new channel
			self.fwData[str(channelIds[val])] = newChannel
		# Need to create a pandas dataframe containing all the data
		channelDict = {}
		for key in self.fwData:
			dataDate = []
			dataValue = []
			for row in self.fwData[key]:
				#print(row['DataValue'])
				dataValue.append(float(row['DataValue']))
				if key == str(channelIds[0]):
					dataDate.append(row['DataTime'])
			if dataDate:
				channelDict['Timestamp'] = dataDate
			channelDict[str(key)] = dataValue
		updatedLength = []
		for key in self.fwData:
			updatedLength.append(len(self.fwData[key]))
		# Convert to dataframe
		etData = pd.DataFrame(channelDict)
		# Convert time string to time object
		etData['Timestamp'] = pd.to_datetime(etData['Timestamp'],format="%Y-%m-%dT%H:%M:%S")
		# Update header names
		etData.columns = ['Timestamp','smMeanTemp','smMaxTemp','smMinTemp','smRelHum','smInsInc','smWindSpeed']
		self.etData = etData
		# Assign variables as global variables to use in later functions
		self.startDate = startTimestamp
		self.endDate = endTimestamp
	
	def getUniqueSiteIDs(self,jobNum='4154.002'):
		sqlQuery = "select distinct(siteNumber) from sites where kwlProject ='+str(jobNum)+' and calcStatus != 'Complete'"
		cursor.execute(sqlQuery)
		sqlData = cursor.fetchall()
		# Convert to list
		ids = [x[0] for x in sqlData]
		
		self.siteIDs = ids

	def getSQLData(self,jobNum,siteID):
		sqlQuery = 'select * from inputTimeSeries where kwlProject='+str(jobNum)+' and siteNumber = '+str(siteID)+' order by dataDateTime'
		cursor.execute(sqlQuery)
		sqlData = cursor.fetchall()
		data = pd.DataFrame([list(x) for x in sqlData])
		data.columns = ['kwlProject','siteNumber','parameter','dateTime','value']
		subsetData = data[data['siteNumber']==siteID]
		# Convert from long to wide
		self.sqlData = subsetData.pivot(index='dateTime',columns='parameter',values='value').reset_index()


	def getMinMaxTime(self):
		# Grab max timestamp
		self.endDate = max(self.sqlData['dateTime'])
		self.startDate = min(self.sqlData['dateTime'])	

	def calcEvapMultiple(self,startTimestamp=None,endTimestamp=None,jobNumber='4154.002',writeResultsSQL=False):
		self.jobNumber = jobNumber
		# First step is to grab list of unique site IDs
		self.getUniqueSiteIDs(jobNumber)
		# Grab the data for one of the station IDs
		for loc in self.siteIDs:
			print('siteID: ',loc)
			self.getSQLData(jobNumber,loc)
			# Check start and end timestamps 
			if (startTimestamp==None) & (endTimestamp==None):
				self.getMinMaxTime()
			else:
				# Check that startTimestamp is within the dataset
				if (startTimestamp >= max(self.sqlData['dateTime'])):
					sys.exit('The start time is greater than the largest value in the SQL table. Please select a smaller value.')
				else:
					# Check that the endTimestamp is not less than the smallest value
					if endTimestamp < min(self.sqlData['dateTime']):
						sys.exit("The end time stamp is less than the smallest value in the SQL table. Please select a larger value.")
					else:
						# Check that end is greater than start
						if startTimestamp > endTimestamp:
							sys.exit("The start timestamp is greater than the end timestamp. Please fix and try again.")
						else:
							self.endDate = endTimestamp
							self.startDate = startTimestamp
							print("hello there")

			
			# Grab lat,long,elev for site
			sqlQuery = 'select * from sites where kwlProject='+str(jobNumber)+'and siteNumber='+str(loc)
			cursor.execute(sqlQuery)
			sqlData = cursor.fetchall()[0]
			lat = sqlData[2]
			lon = sqlData[3]
			elev = sqlData[4]
			alb = sqlData[5]
			self.etData = self.sqlData[(self.sqlData['dateTime']>=self.startDate) & (self.sqlData['dateTime']<=self.endDate)]
			self.calcEvap(smMaxTemp='T2M_MAX',smMinTemp='T2M_MIN',smWindSpeed='WS10M',smRelHum='RH2M',timestamp='dateTime',
				smInsInc='ALLSKY_SFC_SW_DWN',writeSQL=writeResultsSQL,siteID=loc,avgElev=elev,latDeg=lat,albedo=alb)

	def calcEvap(self,smMaxTemp='smMaxTemp',smMinTemp='smMinTemp',smWindSpeed='smWindSpeed',smRelHum='smRelHum',
		timestamp='Timestamp',smInsInc='smInsInc',avgElev=710,latDeg=49.502402,albedo=0.2,solar=0.082,boltzman=4.903*10**-9,latentHeatVap=2.45,writeSQL=False,siteID=000000):
		# Calculate parameters
		latRad = np.pi/180*latDeg
		atmPress = 101.3*((293-0.0065*avgElev)/293)**(5.26)
		psycho = 0.00163/latentHeatVap*atmPress
		# Calculate mean temp
		self.etData['meanTemp'] = (self.etData[smMaxTemp] + self.etData[smMinTemp])/2
		# Calculate wind speed
		self.etData['windSpeed'] = self.etData[smWindSpeed]*(4.87/np.log(67.8*10-5.42))
		# Calculate slope of vapor pressure curve
		self.etData['slpVaporCurve'] = 4098*(0.6108*np.exp((17.27*self.etData['meanTemp'])/(237.3+self.etData['meanTemp']))/(self.etData['meanTemp']+237.3)**2)
		# Calculate delta term
		self.etData['deltaTerm'] = self.etData['slpVaporCurve']/(self.etData['slpVaporCurve']+psycho*(1+0.34*self.etData['windSpeed']))
		# Calculate psi term
		self.etData['psiTerm'] = psycho/(self.etData['slpVaporCurve']+psycho*(1+0.34*self.etData['windSpeed']))
		# Calculate temp term
		self.etData['tempTerm'] = (900/(self.etData['meanTemp']+273))*self.etData['windSpeed']
		# Calculate max sat vapor pressure
		self.etData['maxSatVapPress'] = 0.6108*np.exp(17.27*self.etData[smMaxTemp]/(self.etData[smMaxTemp]+237.3))
		# Calculate min sat vapor pressure
		self.etData['minSatVapPress'] = 0.6108*np.exp(17.27*self.etData[smMinTemp]/(self.etData[smMinTemp]+237.3))
		# Calculate mean sat vapor pressure
		self.etData['meanSatVapPress'] = (self.etData['maxSatVapPress']+self.etData['minSatVapPress'])/2
		# Calculate actual vapor pressure
		self.etData['actualVapPress'] = self.etData[smRelHum]/100*self.etData['meanSatVapPress']
		# Determine day of year
		self.etData['dayOfYear'] = self.etData[timestamp].dt.dayofyear
		# Calculate inverse rel dist earth-sun
		self.etData['invRelDist'] = 1+0.033*np.cos(2*np.pi/365*self.etData['dayOfYear'])
		# Calculate solar declination
		self.etData['solarDeclin'] = 0.409*np.sin(2*np.pi/365*self.etData['dayOfYear']-1.39)
		# Calculate sunset hour angle
		self.etData['sunsetHrAng'] = np.arccos(-np.tan(latRad)*np.tan(self.etData['solarDeclin']))
		# Calculate extraterrestial radiation
		self.etData['extraRadiation'] = 24*60/np.pi*solar*self.etData['invRelDist']*(self.etData['sunsetHrAng']*np.sin(latRad)*np.sin(self.etData['solarDeclin'])+np.cos(latRad)*np.cos(self.etData['solarDeclin'])*np.sin(self.etData['sunsetHrAng']))
		# Calculate clear sky solar radiation
		self.etData['clearSkyRadiation'] = (0.75+2*10**(-5)*avgElev)*self.etData['extraRadiation']
		# Calculate net shortwave radiation
		self.etData['netShortRadiation'] = (1-albedo)*self.etData[smInsInc]*3.6
		# Calculate net outgoing longwave radiation
		self.etData['netLongRadiation'] = boltzman*((self.etData[smMaxTemp]+273.16)**4+(self.etData[smMinTemp]+273.16)**4)/2*(0.34-0.14*self.etData['actualVapPress']**0.5)*(1.35*self.etData[smInsInc]*3.6/self.etData['clearSkyRadiation']-0.35)
		# Calculate net radiation equivalent evapotranspiration
		self.etData['netEquivEvapo'] = (self.etData['netShortRadiation']-self.etData['netLongRadiation'])*0.408
		# Calculate radiation term
		self.etData['radiationTerm'] = self.etData['deltaTerm']*self.etData['netEquivEvapo']
		# Calculate wind term
		self.etData['windTerm'] = self.etData['psiTerm']*self.etData['tempTerm']*(self.etData['meanSatVapPress']-self.etData['actualVapPress'])
		# Calculate reference evapotranspiration
		self.etData['refEvapo'] = self.etData['radiationTerm'] + self.etData['windTerm']
		# Calculate adjusted reference evapotranspiration
		self.etData['adjRefEvapo'] = self.etData['refEvapo'].apply(lambda x: 0 if x < 0 else x)
		# Grab only data that is greater than or equal to startTimestamp
		midnightStartDate = self.startDate.replace(hour=0,minute=0,second=0)
		self.etDataShort = self.etData.loc[self.etData[timestamp]>=midnightStartDate]
		# Split Timestamp and adjRefEvapo into their own lists
		tsDate = self.etDataShort[timestamp].tolist()
		# Convert tsDate to string
		self.tsDate = []
		for ts in tsDate:
			self.tsDate.append(ts.strftime("%m/%d/%Y %H:%M"))
		self.tsEvap = self.etDataShort['adjRefEvapo'].tolist()
		# Write results to sql
		sqlQuery = 'insert into outputTimeSeries (kwlProject,siteNumber,parameter,dataDateTime,value) values(?,?,?,?,?)'
		if writeSQL:
			print("Writing time series into SQL...")
			for ii,jj in zip(self.tsDate,self.tsEvap):
				#print(sqlQuery,(self.jobNumber,siteID,'et',ii,jj))
				cursor.execute(sqlQuery,(self.jobNumber,siteID,'et',ii,jj))
				#sql = "update "+str(runcontroltable)+" set runstatus = 'Run Failed "+str(date_time)+"', service = '"+str(sys.argv[1])+"' where transect = '"+str(self.scenarioVars.transect)+"' and wl = "+str(self.scenarioVars.wl)+" and winddir = "+str(self.scenarioVars.winddir)+" and rp = "+str(self.scenarioVars.rp)+" and method='"+self.scenarioVars.method+"'"
			sqlQuery = "update sites set calcStatus= 'Complete' where kwlProject='"+str(self.jobNumber)+"' and siteNumber='"+str(siteID)+"'"
			print(sqlQuery) 
			cursor.execute(sqlQuery)
			cursor.commit()
			#sys.exit()






