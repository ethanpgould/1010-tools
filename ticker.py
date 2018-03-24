# author: ethan p gould
# email: ethan@plpfunds.com
# for internal use of Pleasant Lake Partners LLC

class Ticker(object):
	"""docstring for Ticker"""

	from operator import itemgetter # used in monthly update

	def __init__(self, name):
		super(Ticker, self).__init__()
		
		self.name = name
		self.weekly_vals = {}
		self.monthly_vals = {}
		self.quarterly_vals = {}

	def add_weeks(self, val_list, w_periods):
		"""
		val_list: [(period, sales_idx), ...]
			taken from a dict 

		w_periods: list of current weekly periods

		"""
		for i in val_list:
			if int(i[0]) in w_periods and i[1]:
				self.weekly_vals[int(i[0])] = float(i[1])


	def add_months(self, val_list, m_periods):
		"""
		val_list: [(period, sales_idx), ...]
			taken from a dict 

		m_periods: list of current weekly periods

		"""
		val_list.sort(key=itemgetter(0)) # sort by first idx of tuple in ascending order 

		# assuming m_periods in descending order 
		for i in m_periods:
			point = val_list.pop()[1]
			if point: # ensure not an empty string 
				self.monthly_vals[i] = float(point)


	def add_quarters(self, val_list, q_periods):
		"""
		val_list: [(period, sales_idx), ...]
			taken from a dict 

		q_periods: list of current weekly periods

		"""
		for i in val_list:
			if int(i[0]) in q_periods and i[1]:
				self.q_periods[int(i[0])] = float(i[1])

	def df_row(self, timeframe):

		if timeframe == "weekly":
			row = pd.Series(data = self.weekly_vals)
			return row
		elif timeframe == "monthly":
			row = pd.Series(data = self.monthly_vals)
			return row
		elif timeframe == "quarterly":
			row = pd.Series(data = self.quarterly_vals)

	





	