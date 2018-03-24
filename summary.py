# author: ethan p gould
# email: ethan@plpfunds.com
# for internal use of Pleasant Lake Partners LLC

class SummarySheet(object):
	"""
	docstring for SummarySheet

	"""

	def __init__(self, name):
		super(SummarySheet, self).__init__()
		
		self.name = name 
		self.ticker_names = set()
		self.tickers = [] # list of ticker objects 

		self.weekly_data = {} 
		self.monthly_data = {}
		self.quarterly_data = {}

		self.cur_weekly_periods = []
		self.cur_monthly_periods = []
		self.cur_quarterly_periods = []


	def structure_from_csv(self, filename, n, timeframe):
	"""
	will digest the contents of query_1010_server saved as a csv
	returns list of dictionaries and list of n most recent periods 

	args: 
		name: 
	rows: tickers
	columns: yoy data 
	"""
	data = {}
	periods = set()

	with open(name, 'r') as file:
		reader = csv.reader(file)

		for row in reader:
			key = row[1][2:-1]
			self.ticker_names.add(key) # build a set of all the ticker names 
									   # same set added to each time stucture is called 
			if key not in data:
				data[key] = [(row[2], row[4])]
			else:
				data[key].append((row[2], row[4]))

			if(row[2].isdigit()):
				periods.add(int(row[2]))
	data.pop('cke') # total bodge but it works 
	current_periods = sorted(list(periods))[-n:]

	# assign to the sheets fields 
	if timeframe == "weekly":
		self.weekly_data = data
		self.cur_weekly_periods = current_periods
	elif timeframe == "monthly":
		self.monthly_data = data
		self.cur_monthly_periods = current_periods
	elif timeframe == "quarterly":
		self.quarterly_data = data
		self.cur_quarterly_periods = current_periods

	# populate each ticker object with weekly, monthly, and quarterly data 
	def fill_tickers():

		for i in ticker_names: 
			t = Ticker(i)

			t.add_weeks(weekly_data[i], cur_weekly_periods)
			t.add_months(monthly_data[i], cur_monthly_periods)
			t.add_quarters(quarterly_data[i], cur_quarterly_periods)

			self.tickers.append(t) # populate a full list of ticker objects 

	# print as a pdf
	def create_df():
		full_df = pd.concat([])

	def weekly_df():
		out = pd.DataFrame(columns = cur_weekly_periods)

		for t in tickers:
			out.loc[t.name] = t.df_row("weekly")

		return out


	def monthly_df():
		out = pd.DataFrame(columns = cur_monthly_periods)

		for t in tickers:
			out.loc[t.name] = t.df_row("monthly")

		return out

	def quarterly_df():
		out = pd.DataFrame(columns = cur_quarterly_periods)

		for t in tickers: 
			out.loc[t.name] = t.df_row("quarterly")

	# export to excel 

	# 