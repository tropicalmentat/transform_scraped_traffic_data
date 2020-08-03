import datetime as dt

data_fpath = r'/home/gtorres/Desktop/transform_scrape_traffic_data/data/20200803/trfc_stat_20200803_121502.csv'

with open(data_fpath, "r") as fobj:
	for i,ln in enumerate(fobj):
		cln_ln = ln.strip('\n')
		if i > 0:
			scrp_ts = cln_ln.split(",")[-1]
			nb_ts = cln_ln.split(",")[3]

			duration = int(nb_ts.split(" ")[2].strip('('))
			unit = nb_ts.split(" ")[3]

			if unit=='seconds':
				td=dt.timedelta(seconds=duration)
			elif unit=='hrs':
				td = dt.timedelta(hours=duration)
			elif unit=="days":
				td = dt.timedelta(days=duration)

			try:
				print(nb_ts,dt.datetime.strptime(scrp_ts,"%Y-%m-%d %H:%M")-td)
			except Exception as e:
				print(e)

			# TODO: replace of derived timestamp with actual time of update