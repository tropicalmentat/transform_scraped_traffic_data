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
	Takes in a raw line_tower from the input data and
	transforms it into analyzable format

	Attributes
	----------
	raw_data : str
		Raw scraped data line item
	cleaned_data : list
		List of string elements from raw_data
	line_tower : list
		Unique location identifier for traffic update
	scrape_timestamp : str
		Timestamp of line item scrape from website
	raw_northbound_timestamp : str
		Timestamp of northbound traffic status update
	raw_southbound_timestamp : str
		Timestamp of southbound traffic status update

	"""

	def __init__(self,raw_data):

		self.raw_data = raw_data
		self.cleaned_data = None
		self.line_tower = None
		self.scrape_timestamp = None

		self.raw_northbound_timestamp = None
		self.raw_southbound_timestamp = None 

		self.northbound_update_timelapse = None
		self.southbound_update_timelapse = None

		self.estimated_northbound_timestamp = None
		self.southbound_estimated_timestamp = None

		self.northbound_raw_minutes = None
		self.southbound_raw_minutes = None

		self.actual_northbound_timestamp = None
		self.actual_southbound_timestamp = None

		self.clean_and_decompose()
		self.get_line_and_tower()
		self.get_scrape_timestamp()
		self.get_northbound_timestamp()
		self.get_southbound_timestamp()

		self.set_estimated_northbound_timestamp(self.raw_northbound_timestamp)
		self.set_estimated_southbound_timestamp(self.raw_southbound_timestamp)

	def clean_and_decompose(self):

		self.cleaned_data = self.raw_data.strip('\n').split(",")

		return

	def get_line_and_tower(self):

		self.line_tower = self.cleaned_data[0:2]

		return

	def get_scrape_timestamp(self):

		self.scrape_timestamp = self.cleaned_data[-1]

		return

	def get_northbound_timestamp(self):

		self.raw_northbound_timestamp = self.cleaned_data[5]

		return

	def get_southbound_timestamp(self):

		self.raw_southbound_timestamp = self.cleaned_data[3]

		return

	def set_estimated_northbound_timestamp(self,timestamp):

		self.estimated_northbound_timestamp = self.calculate_estimated_timestamp(timestamp)

		return

	def set_estimated_southbound_timestamp(self,timestamp):

		self.estimated_southbound_timestamp = self.calculate_estimated_timestamp(timestamp)

		return

	def calculate_estimated_timestamp(self,timestamp):


		def get_minutes(timestamp):

			return timestamp.split(" ")[0].split(":")[1]

		def get_update_duration(timestamp):
			
			return int(timestamp.split(" ")[2].strip('('))
			
		def get_duration_unit(timestamp):

			return timestamp.split(" ")[3]


		duration = get_update_duration(timestamp)
		unit = get_duration_unit(timestamp)

		if unit=='seconds':
			_timedelta = dt.timedelta(seconds=duration)
		elif unit=='mins':
			_timedelta = dt.timedelta(minutes=duration)		
		elif unit=='hrs':
			_timedelta = dt.timedelta(hours=duration)
		elif unit=="days":
			_timedelta = dt.timedelta(days=duration)

		return dt.datetime.strptime(self.scrape_timestamp,"%Y-%m-%d %H:%M")-_timedelta



def main():
	data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_214502.csv'

	with open(data_fpath, "r") as fobj:
		for i,ln in enumerate(fobj):
			cln_ln = ln.strip('\n')

			line_tower = ' '.join(ln.split(',')[0:2])

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
				
				print(line_tower,estimated_ts,actual_ts)
				# TODO: Need to generalize extraction of timestamps from northbound and southbound data
				# TODO: Deduplication of records


def _main():

	data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_214502.csv'

	with open(data_fpath, "r") as fobj:
		for i,ln in enumerate(fobj):
			if i==0:
				pass
			else:
				transformed = transform_timestamp(ln)
				print(transformed.estimated_southbound_timestamp)

if __name__=="__main__":
	_main()