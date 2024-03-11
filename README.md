## Intro
This script grabs data from the FlowWorks API for specified stations (DNV NASA SolarMet) and channels and calculates a daily time series of evapotranspiration. 

## Assumptions
- Any channel with missing data near the end of the user specified end timestamp, has its missing days replaced with the most previous available data in the channel. The script retrieves an additional 30 days of historical data to help in filling in missing data. 
- Any other missing days in the time series are replaced by the previous value. 
- All retrieved data are daily timestamps. 

## User Inputs
- `stationId`
	+ Integer number representing the station ID as defined by FlowWorks
- `channelIds`
	+ This is a list of channel ids from the station of interest
- `startTimestamp`
	+ This is a datetime object
- `endTimestamp`
	+ This is a datetime objects
- `apiVersion`
	+ String that takes either V1 or V2 as inputs to determine which FlowWorks api to use.
- `days`
	+ Varible to grab data past the startTimestamp. Default is set to 30.

## Metadata
Script was written by LCG for JV as part of the SWMM automation model

- Developer(s): LCG
- Technical reviewer(s): None
- Python reviewer(s): None
- Date of review: YYYY-MM-DD
- Date of last update: 2021-03-19
- Version: 1.0.1

