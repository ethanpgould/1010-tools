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
# homecooked objects, must be in containing directory 
from ticker import Ticker
from summary import SummarySheet

pd.set_option('expand_frame_repr', False)

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

	name = cardtype + timeframe + "updated.csv"
	query = ''                                                                                               
	# run the query
	q = s.query(tbl, query)                                       # define query
	q.run()                                                       # run query
	df = pd.DataFrame(q.dictslice(0,1000000))                     # capping size of download
	# print(df)                                                   # print output
	df.to_csv(name)                                               # write to csv
	return name


def main():

	credit_summary = SummarySheet("credit")

	credit_summary.structure_from_csv(query_1010_server("credit", "weekly"), 6, "weekly")
	credit_summary.structure_from_csv(query_1010_server("credit", "monthly"), 6, "monthly")
	credit_summary.structure_from_csv(query_1010_server("credit", "quarterly"), 6, "quarterly")

	credit_summary.fill_tickers()




	# debit_summary = SummarySheet("debit")


if __name__ == '__main__':
	main()








