# In[Prepare]:

import pandas
import xlwings
import sqlite3

from dolphin import Dolphin
from datetime import *
from dateutil.relativedelta import *
from logbook import Logger, StreamHandler
from multiprocessing.dummy import Pool as ThreadPool

pool = ThreadPool(5)

sas={
'output': Dolphin(r'C:\NotBackedUp\SAS\output'),
'code': Dolphin(r'C:\NotBackedUp\SAS\sas_code'),
'source': Dolphin(r'H:\sas_source'),
'sas_rep': Dolphin(r'H:\sasrep')}

time={
'next' : datetime.now() + relativedelta(months=+1,day=1),
'now' : datetime.now() + relativedelta(days=-1,day=1),
'mid' : datetime.now() + relativedelta(months=-1,weekday=TH(+3),day=1),
'last' : datetime.now() + relativedelta(months=-1,days=-1,day=1)}

str_next={
'month':time['next'].strftime('%b')}
str_now={
'sas':time['now'].strftime('%Y%m%d'),
'dir':time['now'].strftime("%b'%y"),
'month':time['now'].strftime('%b'),
'wks':time['now'].strftime('%m%d')}
str_mid={
'sas':time['mid'].strftime('%Y%m%d'),
'wks':time['mid'].strftime('%m%d')}
str_last={
'sas':time['last'].strftime('%Y%m%d'),
'dir':time['last'].strftime("%b'%y"),
'month':time['last'].strftime('%b'),
'wks':time['last'].strftime('%m%d')}

desktop = Dolphin.environment('desktop')
mysql = sqlite3.connect(r'P:\Desktop\SP\Sales_Performance_{sas_d}.db'.format(sas_d=str_now['month']))

def read_to_sql(source,file_name,**kwargs):
    if kwargs['method'] == 'pcsv':
        df_all = pandas.read_csv(source.join(file_name).path,
                                 header=kwargs['header'], 
                                 sep=kwargs['sep'],
                                 index_col=kwargs['index'],
                                 skiprows=kwargs['skiprows'])
    elif kwargs['method'] == 'pexcel':
        df_all = pandas.read_excel(source.join(file_name).path,
                                   sheetname=kwargs['sheetname'],
                                   header=kwargs['header'],
                                   index_col=kwargs['index'],             
                                   skiprows=kwargs['skiprows'],
                                   parse_cols=kwargs['parse_cols'],
                                   convert_float=kwargs['convert_float'])
    else:
        wks = xlwings.Book(source.join(file_name).path).sheets(kwargs['sheetname'])      
        df_all = wks.range(kwargs['ranges']).options(pandas.DataFrame, 
                          expand=kwargs['expand'], 
                          header=kwargs['header'],
                          index=kwargs['index']).value

    if kwargs['funcs'] is not None:
        for func in kwargs['funcs']:
            df = func(df_all)
    else:
        df = df_all
            
    name = file_name.split('.')[0]
    Logger(file_name).info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
    
    df.to_sql(con=mysql, name=name,index=kwargs['index'])
    
    if kwargs['method'] == 'xlwings':
        wks.book.app.quit()
    
# In[Clint]:
source = sas['source'].finacle
file_name = 'CLINTCB_{sas_d}.TXT'.format(sas_d=str_now['sas'])            
read_to_sql(source,file_name,
            method='pcsv',header=None,sep='|',index=False,skiprows=0,
            funcs=None)

# In[Cloan]:
file_name = 'CLOANCL_{sas_d}.TXT'.format(sas_d=str_now['sas'])
read_to_sql(source,file_name,method='pcsv',
            header=None,sep='|',index=False,skiprows=0,
            funcs=[lambda x:x[x.iloc[:,7]>20170131]])

# In[FTS]:
source = Dolphin(r'\\svrcn166mlp00\general\IntCN\Retail\Sales and Distribution\SMO Report\FTS report\FTS Monthly Report 2017')
file_name = 'FTS reporting monthly {sas_d} revised.xlsx'.format(sas_d=str_now['month'])
def func(x):
    x.columns = 'FTS Account Name', 'Student Name', 'CIF number', 'Passport number', 'Application No', 'Issuing Date', 'Validity Year', 'CNY', 'Issuing Branch', 'Remarks', 'Currency'
    return x
read_to_sql(source,file_name,method='xlwings',
            sheetname='Monthly',expand='down',header=False,index=True,
            funcs=[func],ranges='A4:L4')

# In[MF Product]:
source = sas['source'].cip
file_name = 'Product Risk Rating.xls'
read_to_sql(source,file_name,method='xlwings',
            sheetname='产品明细',expand='table',header=False,index=True,
            ranges='A1',
            funcs=[lambda x:x.query("投资类型=='境外基金'"),lambda x:x.reset_index(inplace=True, drop=True)])
# In[Customer List]:

source = sas['output'].join('Customer List')
file_name = 'Customer List_{sas_d}.xlsx'.format(sas_d=str_now['sas'])
df = pandas.read_excel(source.join(file_name).path,sheetname='Customer List', index_col='CIF')
df['Branch']=0
df['Role']=0
df['SPB new']=0
df['SPB new last']=0
Logger(file_name).info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df[['RM_Code','Branch','Role','Deposit','CASA','FTS','TD','DCI','SD','Insurance','MF','Bond','AUA','NTB','SPBFlag','SPB new','SPB new last','CIF_For_QSPB','AcctOpenDate','QSPB','WealthClient','Deposit_YearEndFxRate','AUM_YearEndFxRate','AUA_YearEndFxRate','AUM_ExcBanca_YearEndFxRate','AUA_ExcBanca_YearEndFxRate','AUM_Prod','Sol_Branch','RM_Branch']].to_sql(con=mysql, name='Cust List_{sas_d}'.format(sas_d=str_now['wks']))

file_name = 'Customer List_{sas_d}.xlsx'.format(sas_d=str_mid['sas'])
df = pandas.read_excel(source.join(file_name).path,sheetname='Customer List', index_col='CIF')
df['Branch']=0
df['Role']=0
df['SPB new']=0
df['SPB new last']=0
Logger(file_name).info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df[['RM_Code','Branch','Role','Deposit','CASA','FTS','TD','DCI','SD','Insurance','MF','Bond','AUA','NTB','SPBFlag','SPB new','SPB new last','CIF_For_QSPB','AcctOpenDate','QSPB','WealthClient','Deposit_YearEndFxRate','AUM_YearEndFxRate','AUA_YearEndFxRate','AUM_ExcBanca_YearEndFxRate','AUA_ExcBanca_YearEndFxRate','AUM_Prod','Sol_Branch','RM_Branch']].to_sql(con=mysql, name='Cust List_{sas_d}'.format(sas_d=str_mid['wks']))

file_name = 'Customer List_{sas_d}.xlsx'.format(sas_d=str_last['sas'])
df = pandas.read_excel(source.join(file_name).path,sheetname='Customer List', index_col='CIF')
df['Branch']=0
df['Role']=0
df['SPB new']=0
df['SPB new last']=0
Logger(file_name).info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df[['RM_Code','Branch','Role','Deposit','CASA','FTS','TD','DCI','SD','Insurance','MF','Bond','AUA','NTB','SPBFlag','SPB new','SPB new last','CIF_For_QSPB','AcctOpenDate','QSPB','WealthClient','Deposit_YearEndFxRate','AUM_YearEndFxRate','AUA_YearEndFxRate','AUM_ExcBanca_YearEndFxRate','AUA_ExcBanca_YearEndFxRate','AUM_Prod','Sol_Branch','RM_Branch']].to_sql(con=mysql, name='Cust List_{sas_d}'.format(sas_d=str_last['wks']))

# In[Casa&TD/AUA&QSPB _last]:

source = Dolphin(r'\\svrcn166mlp00\general\IntCN\Retail MIS\Incentive_v\Affluent Banking BUIP\FY17\2Q')
file_name = "Sales Performance in {sas_d} v1.xlsx".format(sas_d=str_last['dir'])
wb = xlwings.Book(source.join(file_name).path)

wks = wb.sheets('CASA&TD')
df = wks.range('A1:L1').options(pandas.DataFrame, index_col='CIF', expand='down').value
Logger('CASA&TD').info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df.to_sql(con=mysql, name='CASA&TD_{sas_d}'.format(sas_d=str_last['month']))
df = wks.range('O1:Q1').options(pandas.DataFrame, index_col='CIF', expand='down').value
Logger('CASA&TD_Pivot').info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df.to_sql(con=mysql, name='CASA&TD_P_{sas_d}'.format(sas_d=str_last['month']),index=False)


wks = wb.sheets('AUA&QSPB')
df = wks.range('A1:X1').options(pandas.DataFrame, index_col='CIF', expand='down').value
Logger('AUA&QSPB').info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df.to_sql(con=mysql, name='AUA&QSPB_{sas_d}'.format(sas_d=str_last['month']),index=False)

wks = wb.sheets('QSPB')
df = wks.range('A1').options(pandas.DataFrame,expand='down').value
Logger('QSPB').info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df.to_sql(con=mysql,name='QSPB_old')

# In[Banca Deal]:
wks = wb.sheets('Banca Deal')
dfl = wks.range('A1:O1').options(pandas.DataFrame, index_col=False,expand='down').value

source = sas['source'].banca               
file_name = 'Banca.xls'
df_all = pandas.read_excel(source.join(file_name).path,sheetname='Banca')
df_all['idx'] = df_all.index
               
name = dfl.iloc[-1]['Customer Name']
gb = df_all.groupby('Customer').last()
x = gb.loc[name]['idx']

df = df_all.loc[x+1:,['CIF','Application Date','Customer','RM','产品类型','AUA',
                      'Currency','Sum Assured','Premium','checking']]
Logger(file_name).info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df.to_sql(con=mysql,name='Banca Deal',index=False)

# In[Target Discount / Weightage]:
file_name = 'FY17 Sales Target_Feb.xls'
df = pandas.read_excel(desktop.join(file_name).path,sheetname='Sheet1')
df.loc[:,'Branch'] = df['Branch'].fillna(method='ffill')
df.columns = ['Branch', 'Idx', 'Team', 'Name', 'CName', 'RM CODE',
              'Title', 'Join Date', 'Staff ID', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
              'Jan', 'Feb', 'Start Selling Date', 'Remarks']
Logger(file_name).info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))
df.to_sql(con=mysql,name='Target Discount',index=False)

grouped=df.groupby('Team')
tempsql = sqlite3.connect(r'P:\Desktop\SP\Teams_{sas_d}.db'.format(
    sas_d=str_now['month']))
for name, group in grouped:
    group.to_sql(con=tempsql,name=name)

wks.book.app.quit()

# In[Wealth Deal]:
# Template
Dolphin.environment('desktop').SP.join('template Wealth Deal.xls').distribute(Dolphin.environment('desktop').path)
wb = xlwings.Book(Dolphin.environment('desktop').join('template Wealth Deal.xls').path)
wks = wb.sheets('CNY')
# Rate
source = Dolphin(r'H:\sas_source\fx rate')
file_name = 'FX RATE {sas_d}.xls'.format(sas_d=str_now['sas'])
df = pandas.read_excel(source.join(file_name).path,
                       skiprows=2, sheetname='Exchange Matrix', 
                       parse_cols='B:N', index_col=0)
df = df['CNY']


last_row = wks.range('A27').end('down').offset(1, 0)
head_row = wks.range('A1')
last_row.value = str_now['month']
last_row.offset(0,1).value = str_now['sas']
var = [df[head_row.offset(0, x).value] for x in range(2, 13)]
for x in range(2, 13):
    last_row.offset(0, x).value = var[x - 2]
    
# Full data

df = pandas.read_excel(Dolphin(r'H:\sas_source\wise\Wealth Deal.xls').path, parse_cols='A:EZ')
wks = wb.sheets('full data')
wks.range('A1').options(pandas.DataFrame, index=False).value = df

source = Dolphin(r'\\svrcn166mlp00\general\IntCN\Retail MIS\Incentive_v\Affluent Banking BUIP\FY17\2Q')
dfl = pandas.read_excel(
    source.join('Wealth Deal {sas_d} for Incentive.xls'.format(sas_d=str_last['dir'])).path,
    sheetname='Next Month WM Deal', skiprows=1)
wks.range('A1').end('down').offset(1, 0).options(pandas.DataFrame, 
         index=False,header=False).value = dfl
         
wks.range('FA2:FD2').api.AutoFill(
    Destination=wks.range('FA2:FD8000').api)
wks.api.Calculate()

df = wks.range('A1').options(pandas.DataFrame, expand='table', index=False).value
df_full = df.query("Date > '2000/1/1'")
df_full = df_full[df_full['Transaction Type'].str.contains('赎回') == False]

Logger('Full Data').info('Row={0},Column={1}'.format(df_full.shape[0],df.shape[1]))

# Next Month WM Deal
df_mf = df_full[df_full['产品类型'] != '双币投资']
df_mf = df_mf[df_mf['确认日'] > pandas.Timestamp(str_now['sas'])]
df_dci = df_full[df_full['产品类型'] == '双币投资']
df_dci = df_dci[df_dci['存款起始日'] > pandas.Timestamp(str_now['sas'])]
df = df_mf.append(df_dci)
wb.sheets('Next Month WM Deal').range('A3').options(pandas.DataFrame,
                                                     index=False,header=False).value = df
Logger('Next Month WM Deal').info('Row={0},Column={1}'.format(df_full.shape[0],df.shape[1]))
# This
wks = wb.sheets('for')
df = df_full.ix[df_full.index.difference(df.index)]
wks.range('A2').options(pandas.DataFrame, index=False, header=False).value = df
df.to_sql(con=mysql,name='WM Deal',index=False)
Logger('for').info('Row={0},Column={1}'.format(df.shape[0],df.shape[1]))

# Save
wb.save(Dolphin.environment('desktop').path + '\\' + 'Wealth Deal {sas_d} for Incentive.xls'.format(sas_d=str_now['sas']))
wb.close()

