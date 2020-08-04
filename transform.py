import datetime as dt
# import great_expectations as ge
import logging

data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_121502.csv'

def get_minutes(timestamp):
	return timestamp.split(" ")[0].split(":")[1]

def get_scrape_timestamp(line_status):
	return cln_ln.split(",")[-1]

def get_northbound_timestamp(line_status):
	return cln_ln.split(",")[3]

def calc_actual_timestamp(timestamp,timedelta):
	return 	dt.datetime.strptime(timestamp,"%Y-%m-%d %H:%M")-timedelta

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
			elif unit=='hrs':
				td = dt.timedelta(hours=duration)
			elif unit=="days":
				td = dt.timedelta(days=duration)

			try:
				actual_ts =  calc_actual_timestamp(scrp_ts,td)
			except Exception as e:
				print(e)

			# TODO: Need to generalize extraction of timestamps from northbound and southbound data
			# TODO: replace of derived timestamp with actual time of update