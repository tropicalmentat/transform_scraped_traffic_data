import datetime as dt
# import great_expectations as ge
import logging

data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_214502.csv'

def get_scrape_timestamp(line_status):
	return cln_ln.split(",")[-1]

def get_northbound_timestamp(line_status):
	return cln_ln.split(",")[3]

def get_minutes(timestamp):
	return timestamp.split(" ")[0].split(":")[1]

def calc_estimated_timestamp(timestamp,timedelta):
	return 	dt.datetime.strptime(timestamp,"%Y-%m-%d %H:%M")-timedelta

def calc_actual_timestamp(timestamp,minutes):
	return dt.datetime(timestamp.year,timestamp.month,timestamp.day,timestamp.hour,int(minutes))

def get_update_timelapse(timestamp):
	duration = int(timestamp.split(" ")[2].strip('('))
	unit = timestamp.split(" ")[3]
	return duration,unit


with open(data_fpath, "r") as fobj:
	for i,ln in enumerate(fobj):
		cln_ln = ln.strip('\n')

		if i > 0:
			scrp_ts = get_scrape_timestamp(cln_ln)
			nb_ts = get_northbound_timestamp(cln_ln)

			nb_ts_mins = get_minutes(nb_ts)

			duration = get_update_timelapse(nb_ts)[0]
			unit = get_update_timelapse(nb_ts)[1]

			if unit=='seconds':
				td = dt.timedelta(seconds=duration)
			elif unit=='mins':
				td = dt.timedelta(minutes=duration)		
			elif unit=='hrs':
				td = dt.timedelta(hours=duration)
			elif unit=="days":
				td = dt.timedelta(days=duration)

			try:
				estimated_ts =  calc_estimated_timestamp(scrp_ts,td)
			except Exception as e:
				print(e)

			actual_ts = calc_actual_timestamp(estimated_ts,nb_ts_mins)
			
			print(estimated_ts,actual_ts)
			# TODO: Need to generalize extraction of timestamps from northbound and southbound data
