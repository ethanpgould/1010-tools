import py1010, csv, pandas as pd

gateway = "http://www2.1010data.com/cgi-bin/gw"
tbl = "default.lonely"
username = "egould_pleasantlake"
password = "3uw3gUDuRh2urb"

queries = [
('creditweekly.csv', r"""
<block name="get_max_file_date" tbl="pub.consumer_data.card_us_v201803.portal.panel1.reports.combined.sales_tracker.fiscal.weekly_yoy">
  
  <base table="{@tbl}"/>
  <note>this is used for the lagged product and applied to the filtered archive tables</note>
  <note>it links and selects the most recent report based on the max file date in the filtered archive tables</note>
  <link table2="{@tbl}" col="file_date" col2="file_date" type="select">
    <note>filter table</note>
    <colord cols="file_date"/>
    <note>find max file date</note>
    <tabu label="Tabulation on File Date" breaks="file_date">
      <break col="file_date" sort="down"/>
      <tcol source="file_date" fun="cnt" label="Count"/>
    </tabu>
    <sel value="i_()=1"/>
    <colord cols="file_date"/>
  </link>
  
  <note>filter all tables for the most recent 8 periods</note>
  <merge/>
  <sort col="reportdate" dir="down"/>
  <sel value="g_cumcnt(ticker;;)<=8"/>

  <note>filter weekly and monthly tables for the most recent 6 periods</note>
  <if test="{endswith('{@tbl}';'monthly' 'weekly' 'monthly_yoy' 'weekly_yoy')=1}">
    <then>
      <sel value="g_cumcnt(ticker;;)<=6"/>
    </then>
  </if>
  
</block>

"""),
('creditmonthly.csv', r"""
<block name="get_max_file_date" tbl="pub.consumer_data.card_us_v201803.portal.panel1.reports.combined.sales_tracker.fiscal.monthly_yoy">
  
  <base table="{@tbl}"/>
  <note>this is used for the lagged product and applied to the filtered archive tables</note>
  <note>it links and selects the most recent report based on the max file date in the filtered archive tables</note>
  <link table2="{@tbl}" col="file_date" col2="file_date" type="select">
    <note>filter table</note>
    <colord cols="file_date"/>
    <note>find max file date</note>
    <tabu label="Tabulation on File Date" breaks="file_date">
      <break col="file_date" sort="down"/>
      <tcol source="file_date" fun="cnt" label="Count"/>
    </tabu>
    <sel value="i_()=1"/>
    <colord cols="file_date"/>
  </link>
  
  <note>filter all tables for the most recent 8 periods</note>
  <merge/>
  <sort col="reportdate" dir="down"/>
  <sel value="g_cumcnt(ticker;;)<=8"/>

  <note>filter weekly and monthly tables for the most recent 6 periods</note>
  <if test="{endswith('{@tbl}';'monthly' 'weekly' 'monthly_yoy' 'weekly_yoy')=1}">
    <then>
      <sel value="g_cumcnt(ticker;;)<=6"/>
    </then>
  </if>
  
</block>

"""),
('creditquarterly.csv', r"""
<block name="get_max_file_date" tbl="pub.consumer_data.card_us_v201803.portal.panel1.reports.combined.sales_tracker.fiscal.quarterly_yoy">
  
  <base table="{@tbl}"/>
  <note>this is used for the lagged product and applied to the filtered archive tables</note>
  <note>it links and selects the most recent report based on the max file date in the filtered archive tables</note>
  <link table2="{@tbl}" col="file_date" col2="file_date" type="select">
    <note>filter table</note>
    <colord cols="file_date"/>
    <note>find max file date</note>
    <tabu label="Tabulation on File Date" breaks="file_date">
      <break col="file_date" sort="down"/>
      <tcol source="file_date" fun="cnt" label="Count"/>
    </tabu>
    <sel value="i_()=1"/>
    <colord cols="file_date"/>
  </link>
  
  <note>filter all tables for the most recent 8 periods</note>
  <merge/>
  <sort col="reportdate" dir="down"/>
  <sel value="g_cumcnt(ticker;;)<=8"/>

  <note>filter weekly and monthly tables for the most recent 6 periods</note>
  <if test="{endswith('{@tbl}';'monthly' 'weekly' 'monthly_yoy' 'weekly_yoy')=1}">
    <then>
      <sel value="g_cumcnt(ticker;;)<=6"/>
    </then>
  </if>
  
</block>

"""),
('debitweekly.csv', r"""
<block name="get_max_file_date" tbl="pub.consumer_data.card_us_v201803.portal.panel2.reports.combined.sales_tracker.fiscal.weekly_yoy">
  
  <base table="{@tbl}"/>
  <note>this is used for the lagged product and applied to the filtered archive tables</note>
  <note>it links and selects the most recent report based on the max file date in the filtered archive tables</note>
  <link table2="{@tbl}" col="file_date" col2="file_date" type="select">
    <note>filter table</note>
    <colord cols="file_date"/>
    <note>find max file date</note>
    <tabu label="Tabulation on File Date" breaks="file_date">
      <break col="file_date" sort="down"/>
      <tcol source="file_date" fun="cnt" label="Count"/>
    </tabu>
    <sel value="i_()=1"/>
    <colord cols="file_date"/>
  </link>
  
  <note>filter all tables for the most recent 8 periods</note>
  <merge/>
  <sort col="reportdate" dir="down"/>
  <sel value="g_cumcnt(ticker;;)<=8"/>

  <note>filter weekly and monthly tables for the most recent 6 periods</note>
  <if test="{endswith('{@tbl}';'monthly' 'weekly' 'monthly_yoy' 'weekly_yoy')=1}">
    <then>
      <sel value="g_cumcnt(ticker;;)<=6"/>
    </then>
  </if>
  
</block>

"""),
('debitmonthly.csv', r"""
<block name="get_max_file_date" tbl="pub.consumer_data.card_us_v201803.portal.panel2.reports.combined.sales_tracker.fiscal.monthly_yoy">
  
  <base table="{@tbl}"/>
  <note>this is used for the lagged product and applied to the filtered archive tables</note>
  <note>it links and selects the most recent report based on the max file date in the filtered archive tables</note>
  <link table2="{@tbl}" col="file_date" col2="file_date" type="select">
    <note>filter table</note>
    <colord cols="file_date"/>
    <note>find max file date</note>
    <tabu label="Tabulation on File Date" breaks="file_date">
      <break col="file_date" sort="down"/>
      <tcol source="file_date" fun="cnt" label="Count"/>
    </tabu>
    <sel value="i_()=1"/>
    <colord cols="file_date"/>
  </link>
  
  <note>filter all tables for the most recent 8 periods</note>
  <merge/>
  <sort col="reportdate" dir="down"/>
  <sel value="g_cumcnt(ticker;;)<=8"/>

  <note>filter weekly and monthly tables for the most recent 6 periods</note>
  <if test="{endswith('{@tbl}';'monthly' 'weekly' 'monthly_yoy' 'weekly_yoy')=1}">
    <then>
      <sel value="g_cumcnt(ticker;;)<=6"/>
    </then>
  </if>
  
</block>

"""),
('debitquarterly.csv', r"""
<block name="get_max_file_date" tbl="pub.consumer_data.card_us_v201803.portal.panel2.reports.combined.sales_tracker.fiscal.quarterly_yoy">
  
  <base table="{@tbl}"/>
  <note>this is used for the lagged product and applied to the filtered archive tables</note>
  <note>it links and selects the most recent report based on the max file date in the filtered archive tables</note>
  <link table2="{@tbl}" col="file_date" col2="file_date" type="select">
    <note>filter table</note>
    <colord cols="file_date"/>
    <note>find max file date</note>
    <tabu label="Tabulation on File Date" breaks="file_date">
      <break col="file_date" sort="down"/>
      <tcol source="file_date" fun="cnt" label="Count"/>
    </tabu>
    <sel value="i_()=1"/>
    <colord cols="file_date"/>
  </link>
  
  <note>filter all tables for the most recent 8 periods</note>
  <merge/>
  <sort col="reportdate" dir="down"/>
  <sel value="g_cumcnt(ticker;;)<=8"/>

  <note>filter weekly and monthly tables for the most recent 6 periods</note>
  <if test="{endswith('{@tbl}';'monthly' 'weekly' 'monthly_yoy' 'weekly_yoy')=1}">
    <then>
      <sel value="g_cumcnt(ticker;;)<=6"/>
    </then>
  </if>
  
</block>

""")
]

s = py1010.Session(gateway, username, password, py1010.POSSESS)

sheets = {}

for name, frame in queries:
	print("Fetching " + name)
	q = s.query(tbl, frame)
	q.run()
	df = pd.DataFrame(q.dictslice(0,1000000))
	df = pd.pivot_table(df, values= 'sales_index_yoy', index=['ticker'], columns=['period'])
	df = df[sorted(df.columns.tolist(), reverse= True)]
	# print(df)
	sheets[name] = df.copy() 

cw = sheets['creditweekly.csv']
cm = sheets['creditmonthly.csv']
cq = sheets['creditquarterly.csv']

creditframe = cw.merge(cm, left_index=True, right_index=True).merge(cq, left_index= True, right_index= True)
print(creditframe)
print("Saving creditframe to .csv")
creditframe.to_csv("full-credit-updated.csv")

dw = sheets['debitweekly.csv']
dm = sheets['debitmonthly.csv']
dq = sheets['debitquarterly.csv']

debitframe = dw.merge(dm, left_index=True, right_index=True).merge(dq, left_index= True, right_index= True)
print(debitframe)
print("Saving debitframe to .csv")
debitframe.to_csv("full-debit-updated.csv")

print('ALL DONE')
