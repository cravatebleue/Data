# In[Prepare]:

import pandas
import xlwings
import numpy
from dolphin import Dolphin
from cuckoo import Cuckoo
import sqlite3


sas_output = Dolphin(r'C:\NotBackedUp\SAS\output')
sas_code = Dolphin(r'C:\NotBackedUp\SAS\sas_code')
sas_source = Dolphin(r'H:\sas_source')
sas_rep = Dolphin(r'H:\sasrep')
desktop=Dolphin.environment('desktop')



# In[time]:
    
time=['2017/02/28','2017/02/16','2017/01/31']
time_this = Cuckoo(time[0]).time
time_mid = Cuckoo(time[1]).time
time_last = Cuckoo(time[2]).time

time_this_sas=time_this.format('YYYYMMDD')      
time_mid_sas=time_mid.format('YYYYMMDD')
time_last_sas=time_last.format('YYYYMMDD')
   
time_this_month=time_this.format('MMM')
time_last_month=time_last.format('MMM')

time_last_dir=time_last.format("MMM'YY")

mydb=sqlite3.connect(r'\\svrcn010ctu0001.global.anz.com\Users$\lid10\Desktop\SP\Sales_Performance_{sas_d}.db'.format(sas_d=time_this_month))
# In[Clint]:

source_finacal=sas_source.finacle
df_clint=pandas.read_csv(source_finacal.join('CLINTCB_{sas_d}.TXT'.format(sas_d=time_this_sas)).path,sep='|',header=None,low_memory=True)
df_clint.to_sql(con=mydb,name='Clint')


# In[Cloan]:

source_finacal=sas_source.finacle
df_cloan=pandas.read_csv(source_finacal.join('CLOANCL_{sas_d}.TXT'.format(sas_d=time_this_sas)).path,sep='|',header=None,low_memory=True)
df_cloan2=df_cloan[df_cloan.iloc[:,7]>20170131]
df_cloan2.to_sql(con=mydb,name='Cloan')


# In[FTS]:

fts_folder=Dolphin(r'\\svrcn166mlp00\general\IntCN\Retail\Sales and Distribution\SMO Report\FTS report\FTS Monthly Report 2017')
wks_fts=xlwings.Book(fts_folder.join('FTS reporting monthly {sas_d} revised.xlsx'.format(sas_d=time_this_month)).path).sheets('Monthly')
df_fts=wks_fts.range('A4:L4').options(pandas.DataFrame,expand='down',header=False).value
df_fts.columns='FTS Account Name','Student Name','CIF number','Passport number','Application No','Issuing Date','Validity Year','CNY','Issuing Branch','Remarks','Currency'
df_fts.to_sql(con=mydb,name='FTS')


# In[MF Product]:

source_cip=sas_source.cip
df_products=pandas.read_excel(source_cip.join('Product Risk Rating.xls').path)
df_MF=df_products[df_products['投资类型']=='境外基金']
df_MF.reset_index(inplace=True,drop=True)
df_MF.to_sql(con=mydb,name='MF Product')


# In[Customer List]:

df_cl_this=pandas.read_excel(sas_output.join('Customer List').join('Customer List_20170228.xlsx').path,sheetname='Customer List',index_col='CIF')
df_cl_mid=pandas.read_excel(sas_output.join('Customer List').join('Customer List_20170216.xlsx').path,sheetname='Customer List',index_col='CIF')
df_cl_last=pandas.read_excel(sas_output.join('Customer List').join('Customer List_20170131.xlsx').path,sheetname='Customer List',index_col='CIF')
df_cl_this.to_sql(con=mydb,name='Customer List_{sas_d}'.format(sas_d=time_this_sas))
df_cl_mid.to_sql(con=mydb,name='Customer List_{sas_d}'.format(sas_d=time_mid_sas))
df_cl_last.to_sql(con=mydb,name='Customer List_{sas_d}'.format(sas_d=time_last_sas))

# - 

# In[Casa&TD_last]:

sales_performance=Dolphin(r'\\svrcn166mlp00\general\IntCN\Retail MIS\Incentive_v\Affluent Banking BUIP\FY17\2Q')
wb_sp_last=xlwings.Book(sales_performance.join("Sales Performance in {sas_d} v1.xlsx".format(sas_d=time_last_dir)).path)
wks_sp_last=wb_sp_last.sheets('CASA&TD')
df_casatd_v=wks_sp_last.range('A1:L1').options(pandas.DataFrame,index_col='CIF',expand='down').value
df_casatd_p=wks_sp_last.range('O1:Q1').options(pandas.DataFrame,index_col='CIF',expand='down').value
df_casatd_v.to_sql(con=mydb,name='CASA_TD_{sas_d}'.format(sas_d=time_last_month))
df_casatd_p.to_sql(con=mydb,name='CASA_TD_P_{sas_d}'.format(sas_d=time_last_month))


# In[AUA&QSPB_last]:

wks_sp_last=wb_sp_last.sheets('AUA&QSPB')
df_aq_v=wks_sp_last.range('A1:X1').options(pandas.DataFrame,index_col='CIF',expand='down').value
df_aq_v.to_sql(con=mydb,name='AUA_QSPB_{sas_d}'.format(sas_d=time_last_month))


# In[AUA&QSPB]:

all_cifs=df_cl_this.append(df_cl_last)
all_cifs2=all_cifs.groupby(all_cifs.index).first()
all_cifs3=all_cifs2[['RM_Code','RM_Branch']]

df_aq_t=all_cifs3.merge(df_cl_this,how='outer',left_index=True, right_index=True,suffixes=('','_{0}'.format(time_this_month)))
df_aq_t2=df_aq_t.loc[:,df_aq_t.columns.str.contains('_{0}'.format(time_this_month))==False]
df_aq_t3=df_aq_t2.fillna(0)
df_aq_t4=df_aq_t3[['RM_Code','AUA_YearEndFxRate','AUA_ExcBanca_YearEndFxRate','Deposit_YearEndFxRate','AUM_ExcBanca_YearEndFxRate','SPBFlag','RM_Branch']]
df_aq_t4.columns=['RM_Code','AUA','AUA (Excl Banca)','Deposit','Deposit (Excl Banca)','SPB','Branch']
df_aq_t4.loc[:,'SPB']=df_aq_t4['SPB'].apply(lambda x:1 if str(x).strip() == 'SPB' else 0)

df_aq_l=all_cifs3.merge(df_cl_last,how='outer',left_index=True, right_index=True,suffixes=('','_{0}'.format(time_last_month)))
df_aq_l2=df_aq_l.loc[:,df_aq_l.columns.str.contains('_{0}'.format(time_last_month))==False]
df_aq_l3=df_aq_l2.fillna(0)
df_aq_l4=df_aq_l3[['AUA_YearEndFxRate','AUA_ExcBanca_YearEndFxRate','Deposit_YearEndFxRate','AUM_ExcBanca_YearEndFxRate','SPBFlag']]
df_aq_l4.columns=['AUA','AUA (Excl Banca)','Deposit','Deposit (Excl Banca)','SPB']
df_aq_l4.loc[:,'SPB']=df_aq_l4['SPB'].apply(lambda x:1 if str(x).strip() == 'SPB' else 0)

df_aq_diff=df_aq_t4-df_aq_l4
df_aq=df_aq_t4.merge(df_aq_l4,how='outer',left_index=True, right_index=True,suffixes=('_Feb','_Jan'))
df_aq1=df_aq.merge(df_aq_diff[['AUA', 'AUA (Excl Banca)', 'Deposit', 'Deposit (Excl Banca)', 'SPB']],how='outer',left_index=True,right_index=True)

df_aq1.loc[:,'QSPB_{sas_d}'.format(sas_d=time_last_month)]=df_aq1.apply(lambda x: 1 if x['AUA_{sas_d}'.format(sas_d=time_last_month)]>280000 and x['SPB_{sas_d}'.format(sas_d=time_last_month)]==1 else 0, axis=1)
df_aq1.loc[:,'QSPB_{sas_d}'.format(sas_d=time_this_month)]=df_aq1.apply(lambda x: 1 if x['AUA_{sas_d}'.format(sas_d=time_this_month)]>280000 and x['SPB_{sas_d}'.format(sas_d=time_this_month)]==1 else 0, axis=1)
df_aq1.loc[:,'NET QSPB']=df_aq1.loc[:,'QSPB_{sas_d}'.format(sas_d=time_this_month)]-df_aq1.loc[:,'QSPB_{sas_d}'.format(sas_d=time_last_month)]

#df_aq.to_sql(con=mydb,name='AUA&QSPB')

# In[QSPB]:

df_qspb_history=wb_sp_last.sheets('QSPB').range('A1:T1').options(pandas.DataFrame,expand='down').value
df_qspb_history1=df_qspb_history.iloc[:,-6:]
df_qspb_history1.loc[:,'upgrade within 6 mths']=df_qspb_history1.apply(lambda x:1 if any(x>0) else 0,axis=1)
df_qspb_history1.loc[:,'upgrade by']=df_qspb_history1[df_qspb_history1['upgrade within 6 mths']==1].apply(lambda x: 0.5 if any(x==0.5) else 1,axis=1)
df_qspb_history2=df_qspb_history1.merge(df_aq1['NET QSPB'],how='outer',left_index=True, right_index=True)

# In[Banca Deal]:

# In[CASA&TD]:
    
    
# In[Wealth Deal]:
# Rate
fx_rate=Dolphin(r'H:\sas_source\fx rate')
df_rate=pandas.read_excel(fx_rate.join('FX RATE {sas_d}.xls'.format(sas_d=time_this_sas)).path,skiprows=2,sheetname='Exchange Matrix',parse_cols='B:N',index_col=0)
df_rate1=df_rate['CNY']
# Template
xlwings.Book(Dolphin.environment('desktop').SP.join('template Wealth Deal.xls').distribute(Dolphin.environment('desktop').path))
wb_template=xlwings.Book(Dolphin.environment('desktop').join('template Wealth Deal.xls').path)    
target_row=wb_template.sheets('CNY').range('A27').end('down').offset(1,0)
follow_row=wb_template.sheets('CNY').range('A1')
# Rate
target_row.value=time_this_month
var =[df_rate1[follow_row.offset(0,x).value] for x in range(2,13)]
for x in range(2,13):
    target_row.offset(0,x).value=var[x-2]
# Full
df_full_data=pandas.read_excel(Dolphin(r'H:\sas_source\wise\Wealth Deal.xls').path,parse_cols='A:EZ')
wb_template.sheets('full data').range('A1').options(pandas.DataFrame,index=False).value=df_full_data
df_for_this=pandas.read_excel(sales_performance.join('Wealth Deal {sas_d} for Incentive.xls'.format(sas_d=time_last_dir)).path,sheetname='Next Month WM Deal',skiprows=1)
wb_template.sheets('full data').range('A1').end('down').offset(1,0).options(pandas.DataFrame,index=False,header=False).value=df_for_this
wb_template.sheets('full data').range('FA2:FD2').api.AutoFill(Destination=wb_template.sheets('full data').range('FA2:FD8000').api)
wb_template.sheets('full data').api.Calculate()
df_full_data1=wb_template.sheets('full data').range('A1').options(pandas.DataFrame,expand='table',index=False).value
df_full_data2=df_full_data1[df_full_data1.Date>'2000/1/1']            
df_full_data3=df_full_data2[df_full_data2['Transaction Type'].str.contains('赎回')==False]
# Next
df_full_data4=df_full_data3[df_full_data3['产品类型']!='双币投资']
df_next=df_full_data4[df_full_data4['确认日']>pandas.Timestamp(time[0])]
df_full_data4=df_full_data3[df_full_data3['产品类型']=='双币投资']
df_next2=df_next.append(df_full_data4[df_full_data4['存款起始日']>pandas.Timestamp(time[0])])
wb_template.sheets('Next Month WM Deal').range('A3').options(pandas.DataFrame,index=False,header=False).value=df_next2
# This
df_this=df_full_data3.ix[df_full_data3.index.difference(df_next2.index)]
wb_template.sheets('for').range('A2').options(pandas.DataFrame,index=False,header=False).value=df_this
# Save
wb_template.save(Dolphin.environment('desktop').path + '\\' + 'Wealth Deal {sas_d} for Incentive.xls'.format(sas_d=time_this_sas))

# In[Cross Sell]:

# - RM Code

# - Target Discount

# - Target Weightage

