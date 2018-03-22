"""
Script for Internal Use of Pleasant Lake Partners, LLC

fetch1010.py:
	will query 1010 API and update credit and debit numbers 
	creating a summary sheet of selecter tickers recent data

author: Ethan P. Gould
email: ethan@plpfunds.com

Usage: 

"""
import py1010, csv, pandas as pd
pd.set_option('expand_frame_repr', False)

# gateway, base table and API credentials
gateway = "http://www2.1010data.com/cgi-bin/gw"
tbl = "default.lonely"
username = "egould_pleasantlake"
password = "3uw3gUDuRh2urb"

credit_tickers = ['AAL','AAP_ACQ_ADJ','AEO','AMZN','AMZN_EX_WHOLEFOODS','AMZN_EGM_1P_WHOLEFOODS',
	'ANF','APRN','ARO','ASNA','BBBY','BBW','BBY','BGFV','BIG','BJRI','BKE','BKS_ACQ_ADJ','BOJA',
	'BOOT_ACQ_ADJ','BURL','BWLD','CAB','CAKE','CAL_FF','CAR','CASY','CHS','CMG','CNK','CRI',
	'CROX','CVS','DAL_OVERALL','DDS','DECK','DIN_APPLEBEES','DKS','DLTH','DNKN','DPZ',
	'DRI_LS_RL_OG_DIVEST_ADJ','DSW','EAT_CHILIS','EFX','EFX_ACQ_ADJ','ETSY','EXPE','EXPN','EXPR',
	'FINL','FIT','FIVE','FL','FLWS','FOSL','FRAN','FRGI','FRSH','FUN','GCO','GCO_JOURNEY',
	'GCO_LIDS','GES','GME','GNC','GPRO','GPS','GRMN','GRPN_ACQ_ADJ','GRUB_Acq_adj','HABT','HBI',
	'HD_ACQ_ADJ','HIBB','HRB','HTZ_COMBINED','INTU','IRG','JACK','JBLU','JCP','JCREW','JILL','JMBA',
	'JWN','KONA','KORS','KSS','LB','LE','LL','LOCO','LOW','LULU','LUV','LZB','M','MCD','MFRM_ACQ_ADJ',
	'MIK','MUSA','NDLS','NKE','NTRI','NWY','ODP','OLLI','ORLY','OUTR','PBPB','PETM','PETS',
	'PIR','PLAY','PLCE','PNRA','PRTY','PSUN','PZZA','QVCA_ZU_ADJ','RGC','RL','ROST','RSH','SBUX',
	'SCVL','SEAS','SFLY_ACQ_ADJ','SFM','SHAK','SHLD','SHOO','SIG_ACQ_ADJ','SIX_ACQ_ADJ','SKX','SONC',
	'SPWH','SWY_EX_FUEL','TACO','TCS','TGT','TIF','TJX','TLRD','TLYS','TPR','TPR_ACQ_ADJ','TRU',
	'TUES','TUMI','UA','UAL','ULTA','URBN','VFC_ACQ_ADJ','VNCE','VRA','VSI','W_ADJ','WEN','WFM',
	'WING','WMT_ACQ_ADJ','WSM','ZOES','ZUMZ']

recent_month_pds = []

def query_1010_server(cardtype, timeframe):
	"""
	will query the 1010 server with requested source and period types
	fesults are placed into a local csv file

	cartype: "credit", "debit"
	timeframe: "quarterly", "monthly", "weekly"
	"""
	# gateway, base table and API credentials
	gateway = "http://www2.1010data.com/cgi-bin/gw"
	tbl = "default.lonely"
	username = "egould_pleasantlake"
	password = "3uw3gUDuRh2urb"

	s = py1010.Session(gateway, username, password, py1010.POSSESS)  

	# API lookup endpoints 
	credit_quarterly_yoy = "pub.consumer_data.card_us.portal.panel1.reports.combined.sales_tracker.quarterly_yoy" 
	credit_monthly_yoy = "pub.consumer_data.card_us.portal.panel1.reports.combined.sales_tracker.monthly_yoy"
	credit_weekly_yoy = "pub.consumer_data.card_us.portal.panel1.reports.combined.sales_tracker.weekly_yoy"

	debit_quarterly_yoy = "pub.consumer_data.card_us.portal.panel2.reports.combined.sales_tracker.quarterly_yoy" 
	debit_monthly_yoy = "pub.consumer_data.card_us.portal.panel2.reports.combined.sales_tracker.monthly_yoy"
	debit_weekly_yoy = "pub.consumer_data.card_us.portal.panel2.reports.combined.sales_tracker.weekly_yoy" 

	if(cardtype == "credit"):
		if(timeframe == "quarterly"):
			tbl = credit_quarterly_yoy
		if(timeframe == "monthly"):
			tbl = credit_monthly_yoy
		if(timeframe == "weekly"):
			tbl = credit_weekly_yoy
	elif(cardtype == "debit"):
		if(timeframe == "quarterly"):
			tbl = debit_quarterly_yoy
		if(timeframe == "monthly"):
			tbl = debit_monthly_yoy
		if(timeframe == "weekly"):
			tbl = debit_weekly_yoy

	name = cardtype + timeframe + "updated-test.csv"
	query = ''                                                                                               
	# run the query
	q = s.query(tbl, query)                                       # define query
	q.run()                                                       # run query
	df = pd.DataFrame(q.dictslice(0,1000000))                     # capping size of download
	# print(df)                                                   # print output
	df.to_csv(name)                                               # write to csv
	return name

def structure_from_csv(name, n):
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

			if key not in data:
				data[key] = [(row[2], row[4])]
			else:
				data[key].append((row[2], row[4]))

			if(row[2].isdigit()):
				periods.add(int(row[2]))
	data.pop('cke') # total bodge but it works 
	current_periods = sorted(list(periods))[-n:]

	return (data, current_periods)

# pd.concat([df1, df2], axis=1) <-- concat df's as columns side by side 
def create_period_df(pair):
	"""
	pair: a (dict, list) tuple
		dict: {ticker: [(period, sales_idx), ...]}
		list: [n most recent periods]

	"""	

	data = pair[0]
	column_periods = pair[1][::-1] # desecending order

	out_df = pd.DataFrame(columns = column_periods)

	for ticker in data:
		# if ticker in credit_tickers:
			values = {}
			for i in data[ticker]: # loop over list tuples
				if int(i[0]) in column_periods and i[1]:
					values[int(i[0])] = float(i[1])
			new = pd.Series(data = values)
			out_df.loc[ticker] = new

	return out_df


# TODO: fix this shitty bodge 
def create_monthly_df(pair):
	"""
	pair: a (dict, list) tuple
		dict: {ticker: [(period, sales_idx), ...]}
		list: [n most recent periods]
		
	"""	
	from operator import itemgetter

	data = pair[0]
	column_periods = pair[1][::-1] # desecending order

	out_df = pd.DataFrame(columns = column_periods)

	for ticker in data:

		values = {}

		data[ticker].sort(key=itemgetter(0)) # sort by first idx of tuple in ascending order 
		
		# print(ticker, '\n') 
		# print(data[ticker])

		for i in column_periods:
			
			pnt = data[ticker].pop()[1]
			if pnt:
				values[i] = float(pnt)

		# print(values)
		# print('\n')

		new = pd.Series(data = values)
		out_df.loc[ticker] = new

	return out_df

# def clean():

def main():
	query_1010_server("credit", "quarterly")
	query_1010_server("credit", "monthly")
	query_1010_server("credit", "weekly")

	weekly_pairs = structure_from_csv("creditweeklyupdated-test.csv", 6)
	weekly_df = create_period_df(weekly_pairs)
	
	monthly_pairs = structure_from_csv("creditmonthlyupdated-test.csv", 6)
	monthly_df = create_monthly_df(monthly_pairs)
	
	quarterly_pairs = structure_from_csv("creditquarterlyupdated-test.csv", 8)
	quarterly_df = create_period_df(quarterly_pairs)
	
	full_credit_df = pd.concat([weekly_df, monthly_df, quarterly_df], axis= 1)
	print(full_credit_df)
	full_credit_df.to_csv("full_credit-test.csv")

	query_1010_server("debit", "quarterly")
	query_1010_server("debit", "monthly")
	query_1010_server("debit", "weekly")

	weekly_pairs = structure_from_csv("debitweeklyupdated-test.csv", 6)
	weekly_df = create_period_df(weekly_pairs)
	
	monthly_pairs = structure_from_csv("debitmonthlyupdated-test.csv", 6)
	monthly_df = create_monthly_df(monthly_pairs)
	
	quarterly_pairs = structure_from_csv("debitquarterlyupdated-test.csv", 8)
	quarterly_df = create_period_df(quarterly_pairs)
	
	full_debit_df = pd.concat([weekly_df, monthly_df, quarterly_df], axis= 1)
	print(full_debit_df)
	full_debit_df.to_csv("full_debit-test.csv")

if __name__ == '__main__':
	main()

	# query_1010_server("debit", "monthly")
	# monthly_pairs = structure_from_csv("debitmonthlyupdated-test.csv", 6)
	# monthly_df = create_monthly_df(monthly_pairs)
	
	# print(monthly_df)
	# monthly_df.to_csv('test.csv')