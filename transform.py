import datetime as dt
# import great_expectations as ge
import logging

def get_scrape_timestamp(line_status):
	return cln_ln.split(",")[-1]

def get_northbound_timestamp(line_status):
	return cln_ln.split(",")[5]

def get_southbound_timestamp(line_status):
	return cln_ln.split(",")[3]

def get_minutes(timestamp):
	return timestamp.split(" ")[0].split(":")[1]

def calc_actual_timestamp(timestamp,minutes):
	return dt.datetime(timestamp.year,timestamp.month,timestamp.day,timestamp.hour,int(minutes))

def get_update_timelapse(timestamp):
	nb_duration = int(timestamp.split(" ")[2].strip('('))
	unit = timestamp.split(" ")[3]
	return nb_duration,unit

def calc_estimated_timestamp(timestamp,timedelta):
	return 	dt.datetime.strptime(timestamp,"%Y-%m-%d %H:%M")-timedelta

class transform_timestamp():
	"""
	Takes in a raw line from the input data and
	transforms it into analyzable format
	"""

	def __init__(self,raw_data):

		self.raw_data = raw_data
		self.cleaned_data = None
		self.line = None
		self.scrape_timestamp = None

		self.northbound_timestamp = None
		self.southbound_timestamp = None 

		self.clean_and_decompose()

	def clean_and_decompose(self):

		self.cleaned_data = self.raw_data.strip('\n').split(",")

		return

	def get_line_and_tower(self):

		self.line = self.cleaned_data[0:2]

		return

	def get_scrape_timestamp(self):

		self.scrape_timestamp = self.cleaned_split(",")[-1]

		return

	def get_northbound_timestamp(self):

		self.northbound_timestamp = self.cleaned_data[5]

		return

	def get_southbound_timestamp(self):

		self.southbound_timestamp = self.cleaned_data[3]



def main():
	data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_214502.csv'

	with open(data_fpath, "r") as fobj:
		for i,ln in enumerate(fobj):
			cln_ln = ln.strip('\n')

			line = ' '.join(ln.split(',')[0:2])

			if i > 0:
				scrp_ts = get_scrape_timestamp(cln_ln)

				nb_ts = get_northbound_timestamp(cln_ln)
				sb_ts = get_southbound_timestamp(cln_ln)

				nb_ts_mins = get_minutes(nb_ts)
				sb_ts_mins = get_minutes(sb_ts)

				# TODO: Package this into a more elegeant function
				nb_duration = get_update_timelapse(nb_ts)[0]
				sb_duration = get_update_timelapse(sb_ts)[0]
				
				unit = get_update_timelapse(nb_ts)[1]

				if unit=='seconds':
					td = dt.timedelta(seconds=nb_duration)
				elif unit=='mins':
					td = dt.timedelta(minutes=nb_duration)		
				elif unit=='hrs':
					td = dt.timedelta(hours=nb_duration)
				elif unit=="days":
					td = dt.timedelta(days=nb_duration)

				try:
					estimated_ts =  calc_estimated_timestamp(scrp_ts,td)
				except Exception as e:
					print(e)

				actual_ts = calc_actual_timestamp(estimated_ts,nb_ts_mins)
				
				print(line,estimated_ts,actual_ts)
				# TODO: Need to generalize extraction of timestamps from northbound and southbound data
				# TODO: Deduplication of records


def _main():

	data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_214502.csv'

	with open(data_fpath, "r") as fobj:
		for i,ln in enumerate(fobj):
			transformed = transform_timestamp(ln)
			print(transformed.cleaned_data)

if __name__=="__main__":
	_main()