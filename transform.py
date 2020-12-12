import datetime as dt
# import great_expectations as ge
import logging

class traffic_status():
	"""

	Takes in a raw line item from the input data and
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
		self.estimated_southbound_timestamp = None

		self.actual_northbound_timestamp = None
		self.actual_southbound_timestamp = None

		self.northbound_status = None
		self.southbound_status = None

		self.northbound_traffic_status = list()
		self.southbound_traffic_status = list()

		self.clean_and_decompose()
		self.get_line_and_tower()
		self.get_scrape_timestamp()
		self.get_northbound_timestamp()
		self.get_southbound_timestamp()

		self.get_northbound_status()
		self.get_southbound_status()

		self.set_estimated_northbound_timestamp(self.raw_northbound_timestamp)
		self.set_estimated_southbound_timestamp(self.raw_southbound_timestamp)

		self.set_actual_northbound_timestamp(self.raw_northbound_timestamp,self.estimated_northbound_timestamp)
		self.set_actual_southbound_timestamp(self.raw_southbound_timestamp,self.estimated_southbound_timestamp)

		self.set_northbound_traffic_status(self.line_tower,
											"NB",
											self.actual_northbound_timestamp,
											self.northbound_status)
		self.set_southbound_traffic_status(self.line_tower,
											"SB",
											self.actual_southbound_timestamp,
											self.southbound_status)

	def clean_and_decompose(self):

		self.cleaned_data = self.raw_data.strip('\n').split(",")

		return

	def get_line_and_tower(self):

		self.line_tower = self.cleaned_data[0:2]

		return

	def get_scrape_timestamp(self):

		self.scrape_timestamp = self.cleaned_data[-1]

		return

	def get_northbound_status(self):

		self.northbound_status = self.cleaned_data[4]

		return

	def get_southbound_status(self):

		self.southbound_status = self.cleaned_data[2]

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

	def set_actual_northbound_timestamp(self,raw,estimate):

		self.actual_northbound_timestamp = self.calculate_actual_timestamp(raw,estimate)

		return

	def set_actual_southbound_timestamp(self,raw,estimate):

		self.actual_southbound_timestamp = self.calculate_actual_timestamp(raw,estimate)

		return

	def calculate_estimated_timestamp(self,timestamp):

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

	def calculate_actual_timestamp(self,raw_timestamp,estimated_timestamp):

		def get_minutes(timestamp):

			return timestamp.split(" ")[0].split(":")[1]

		minutes = get_minutes(raw_timestamp)

		return dt.datetime(estimated_timestamp.year,
							estimated_timestamp.month,
							estimated_timestamp.day,
							estimated_timestamp.hour,
							int(minutes)
							)

	def build_traffic_status(self,line_tower,direction,actual_timestamp,line_status):

		str_actual_timestamp = str(actual_timestamp) # stringify actual timestamp from datetime

		traffic_status = list()
		traffic_status.extend(line_tower)
		traffic_status.append(direction)
		traffic_status.append(str_actual_timestamp)
		traffic_status.append(line_status)

		return traffic_status

	def set_northbound_traffic_status(self,line_tower,direction,actual_timestamp,line_status):



		self.northbound_traffic_status = self.build_traffic_status(
												line_tower,
												direction,
												actual_timestamp,
												line_status
											)

		return

	def set_southbound_traffic_status(self,line_tower,direction,actual_timestamp,line_status):

		self.southbound_traffic_status = self.build_traffic_status(
												line_tower,
												direction,
												actual_timestamp,
												line_status
											)

		return

def main():

	data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_214502.csv'

	with open(data_fpath, "r") as fobj:
		for i,ln in enumerate(fobj):
			if i==0:
				pass
			else:
				transformed = traffic_status(ln)
				print(transformed.northbound_traffic_status)
				print(transformed.southbound_traffic_status)

if __name__=="__main__":
	main()