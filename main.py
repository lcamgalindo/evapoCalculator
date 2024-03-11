# Import libraries
from model import etModel
import datetime
import matplotlib.pyplot as plt

################### FLowWorks Example #####################
# Set station id
stationId = 19098
# Set channel ids
channelIds = [12853,12858,12859,12857,12852,12855] # [avg temp, max temp, min temp, RH, insol incident, wind speed]
# Set start/end datetime objects
startDate = datetime.datetime(2021,11,1,00,00,00)
endDate = datetime.datetime(2021,11,30,00,00,00)
# Initiate model for ET calcs
et = etModel()
# Get data from FlowWorks
et.getFWData(startTimestamp=startDate,endTimestamp=endDate,
	stationId=stationId,channelIds=channelIds,apiVersion="v2",days=30)
# Calculate evapotranspiration time series
et.calcEvap()
# Print lists
#print(et.tsDate,et.tsEvap)

print(et.etDataShort)

et.etData.plot(x='Timestamp',y='adjRefEvapo')
plt.show()
# Write to CSV
#et.etData.to_csv("allData.csv")

############################################################

################### SQL Database Example ###################
# Initiate model for ET calcs
#et = etModel()
# Run evap calculation for all sites in database
#et.calcEvapMultiple(jobNumber='4154.002',writeResultsSQL=True)

###########################################################

