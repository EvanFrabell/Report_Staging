import csv
import os
from datetime import date

import numpy as np
import openpyxl
import pandas as pd
from openpyxl import Workbook

pd.options.display.max_columns = None


# pd.options.display.max_rows = None


class TransformMaster:
    def __init__(self, download, stage, master, power_bi, s_month, archive_date):
        self.download = download
        self.stage = stage
        self.master = master
        self.power_bi = power_bi
        self.df_dl = pd.read_csv(self.download, '\t', low_memory=False)
        self.df_master = pd.read_csv(self.master, low_memory=False)
        self.s_month = s_month
        self.archive_path = f'Master/Archive/{archive_date}/'

        dir_exists = os.path.exists(self.archive_path)
        if not dir_exists:
            os.makedirs(self.archive_path)

    def ecom(self, p_map, l_map, g_map, price_map):
        df = self.df_dl

        # Future Scrape -- Filter
        # for 1000+ logos served with no partner name, search for partner name in browser by subscriberID
        blanks = df[(df['partner'].isna()) & (df['Logos_served'] > 1000)]
        print(blanks[['partner', 'SubscriberId', 'Logos_served']])
        print('!!!!!!!!!!!!!!!!!!FILL IN THE ecomm_fact.tsv SHEET WITH MISSING PARTNERS!!!!!!!!!!!!!!!!!!!')

        df = df[df['partner'].isna() == False]
        # This removes blanks/nulls as well...
        df = df[df['partner'].str.startswith('1W') == False]
        df = df[df['partner'].str.startswith('Dev Sandbox') == False]
        df = df[df['Program'].str.contains('PLEASE_CHECK') == False]

        df['Month'] = self.s_month
        df['Concat'] = df['Level_1'].astype(str) + df['Level_2'].fillna('').astype(str)
        df = pd.merge(df, p_map, left_on='partner', right_on='Partner (Pre-aggregated)', how='left')
        df = pd.merge(df, l_map, left_on='Lcid', right_on='Lcid', how='left')
        df = pd.merge(df, g_map, left_on='CountryName', right_on='Country Name', how='left')
        df = pd.merge(df, price_map, left_on='Concat', right_on='concat', how='left')

        df['Unit Price'] = np.where(df['Add_to_cart_qty'] > 0, df['price'], np.nan)
        df['Price_Measure'] = df['Unit Price'].str.replace('$', '')
        df['Price_Measure'] = df['Price_Measure'].str.replace(',', '').astype(float)
        df['Total Revenue'] = df['Add_to_cart_qty'] * df['Price_Measure']

        df['Partner Type'] = np.where(df['Partner Type'].isnull(), 'Unmapped', df['Partner Type'])

        df = df.drop('Partner (Pre-aggregated)', axis=1)
        df = df.drop('Region', axis=1)

        df = df.rename(
            columns={'FiscalYear': 'Fiscal Year', 'FiscalQuarter': 'Fiscal Quarter',
                     'CountryName': 'User Country', 'CrossSell_Manufacturer': 'Cross Sell SKU Mfr',
                     'Level_1': '1 - Product Type', 'Level_2': '2 - Product Version',
                     'Level_3': '3 - License/Product Type', 'Level_4': '4 - License Count/Yrs',
                     'Logos_served': 'Logos Served', 'Hover_impression': 'Hover Impressions',
                     'Hover_rate': 'Hover Rate', 'View_details': 'View Details', 'Add_to_cart': 'Add to Cart',
                     'Add_to_cart_qty': 'Add to Cart Qty w/ Multiplier', 'Add_to_cart_qty_base': 'Add to Cart Qty',
                     'ActionContext': 'Action Context', 'SubscriberId': 'Subscriber ID', 'Lcid': 'LCID',
                     'partner': 'Partner (Pre-aggregated)', 'MS Region': 'Region'})

        df.insert(15, 'Delete?', '', False)
        df.insert(15, 'DeleteTester', '', False)
        df.insert(15, 'Stopped/NotLive', '', False)

        columns = ['Program', 'Fiscal Year', 'Fiscal Quarter', 'Month', 'Partner Type', 'User Country', 'Region',
                   'Cross Sell SKU Mfr', '1 - Product Type', '2 - Product Version', '3 - License/Product Type',
                   '4 - License Count/Yrs', 'Logos Served', 'Hover Impressions', 'Hover Rate', 'View Details',
                   'Add to Cart', 'Add to Cart Qty w/ Multiplier', 'Add to Cart Qty', 'Action Context', 'Subscriber ID',
                   'LCID', 'Partner Name (Aggregated)', 'Account Type', 'Unit Price', 'Total Revenue', 'Concat',
                   'Partner (Pre-aggregated)', 'Delete?', 'DeleteTester', 'Stopped/NotLive']

        df = df[columns]
        df2 = pd.concat([self.df_master, df])
        df2['Month'] = "=\"" + df2['Month'] + "\""

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv(f'{self.archive_path}ecomm_raw.csv', index=False, encoding='utf-8')
        df2.to_csv('Workbench/Microsoft/ecomm_raw.csv', index=False, encoding='utf-8')

        df3 = df[['Program', 'Fiscal Year', 'Partner Type', 'User Country', 'Region',
                  'Cross Sell SKU Mfr', '1 - Product Type', '2 - Product Version', '3 - License/Product Type',
                  '4 - License Count/Yrs', 'Logos Served', 'Hover Impressions', 'Hover Rate', 'View Details',
                  'Add to Cart', 'Add to Cart Qty w/ Multiplier', 'Add to Cart Qty', 'Partner Name (Aggregated)',
                  'Unit Price', 'Total Revenue']]

        month = self.s_month.split('-')
        year = int(date.today().strftime("%Y"))
        df3.insert(2, 'Month_text', month[1], False)
        df3.insert(2, 'Month_num', month[0], False)

        if int(month[0]) in [1, 2, 3, 4, 5, 6]:
            year -= 1

        df3.insert(2, 'Calander Year', year, False)

        df4 = pd.read_csv('Master/eCOMM_source.csv', low_memory=False)
        df4 = pd.concat([df4, df3])
        df4.to_csv(f'{self.archive_path}eCOMM_source.csv', index=False, encoding='utf-8')
        df4.to_csv(self.power_bi, index=False, encoding='utf-8')

    def epson(self):
        df = self.df_dl
        columns = ['month', 'yr', 'subscriberid', 'partner', 'Country', 'Action', 'Steps', 'Steps_selected',
                   'Conversion_MFR', 'Conversion_PN', 'EventCount']
        df = df[columns]

        df2 = pd.concat([self.df_master, df])

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv(f'{self.archive_path}epson_raw.csv', index=False, encoding='utf-8')
        df2.to_csv(f'Workbench/Epson/epson_raw.csv', index=False)

    def surface(self, partner_map, product_map):
        df = self.df_dl
        df.insert(2, 'Mon', self.s_month, False)
        df = pd.merge(df, partner_map, left_on='partner_name', right_on='Partner (Pre-aggregated)', how='left')
        df = pd.merge(df, product_map, left_on='product_sku', right_on='PN', how='left')
        df = df.rename(columns={'Account Type': 'Account'})
        df = df[['month', 'year', 'Mon', 'program', 'access_country_name', 'requested_language', 'partner_name', 'skey',
                 'Account', 'mfr_name', 'product_sku', 'Product',
                 'Cross_Sell_SKU_Manufacturer', 'Cross_Sell_SKU_PN', 'Title', 'Price', 'impression', 'interaction',
                 'viewport', 'viewdetail', 'atc']]

        df2 = pd.concat([self.df_master, df])
        df2['Mon'] = "=\"" + df2['Mon'] + "\""

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/surface_raw.csv', index=False, encoding='utf-8')
        df2.to_csv(f'{self.archive_path}surface_raw.csv', index=False, encoding='utf-8')
        df2.to_excel(self.power_bi, sheet_name='raw', index=False, header=True, encoding='utf-8')

    def xbox(self, product_map):
        df = self.df_dl
        month = self.s_month.split('-')
        df.insert(3, 'month_t', month[1], False)
        df.insert(3, 'month_num', int(month[0]), False)
        df = pd.merge(df, product_map, left_on='pn', right_on='PN', how='left')
        df = df.rename(columns={'Model': 'Mode'})
        df = df[['program', 'month', 'year', 'month_num', 'month_t', 'mfr_name', 'partner', 'pn', 'Mode',
                 'access_country_name', 'requested_language', 'Logos_served', 'Logos_interaction', 'Hover_impression',
                 'View_details', 'Add_to_cart']]

        df2 = pd.concat([self.df_master, df])

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/xbox_raw.csv', index=False, encoding='utf-8')
        df2.to_csv(f'{self.archive_path}xbox_raw.csv', index=False, encoding='utf-8')
        df2.to_excel(self.power_bi, sheet_name='raw', index=False, header=True, encoding='utf-8')

    def dccn_asset(self, campaign_topic_map, asset_code):
        df = self.df_dl
        month = self.s_month.split('-')
        long_month = month_full(month[1])
        df.insert(0, 'month', self.s_month, False)
        df.insert(2, 'DisplayMonth', long_month, False)
        df['d'] = df['DownloadType'] + df['Asset']

        df = df.loc[df['SubscriberName'] != 'Dev Sandbox // DEV']
        # df = df[df['SubscriberName'].str.startswith('Dev Sandbox') == False]

        df['Partner Type'] = df['ParentTopicName']

        df = pd.merge(df, campaign_topic_map, left_on='Asset', right_on='Asset', how='left')
        df = pd.merge(df, asset_code, left_on='d', right_on='Concat', how='left')
        df = df.rename(columns={'Year': 'Fiscal Year', 'TopicName': 'Asset Sub', 'TopicSource': 'Partner Name',
                                'ParentTopicName': 'partnertype', 'Asset': 'Asset Name', 'EventCount': 'eventcount',
                                'Campaign Topic': 'Asset Topic', 'downloadtype': 'Asset Type',
                                'code': 'Asset Code', 'Level2Name': '2nd Level Sub-Topic'})

        df['Asset Sub-Topic'] = df['Campaign Name']
        df.insert(6, 'countryname', '', False)
        df.insert(6, 'countrystage', '', False)
        df.insert(6, 'region', '', False)

        df = df[['month', 'Fiscal Year', 'DisplayMonth', 'eventcount', 'Partner Name', 'countryname', 'countrystage',
                 'region', 'partnertype', 'Asset Type', 'Asset Name', 'Partner Type', 'Asset Topic', 'Asset Sub',
                 'Asset Sub-Topic', '2nd Level Sub-Topic', 'Asset Code', 'd']]

        df2 = pd.concat([self.df_master, df], ignore_index=True)

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/dccn_simplified_asset_raw.csv', index=False, encoding='utf-8')
        df2.to_csv(f'{self.archive_path}dccn_simplified_asset_raw.csv', index=False, encoding='utf-8')

        df = df.rename(columns={'Fiscal Year': 'year', 'DisplayMonth': 'Month_Num'})
        df.insert(12, 'Campaign Sponsor', 'Microsoft', False)
        df['Month_Num'] = int(month[0])
        df3 = pd.read_csv('Master/DCCN_Asset data_source.csv', low_memory=False)
        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8')
        df3.to_csv(f'{self.archive_path}DCCN_Asset data_source.csv', index=False, encoding='utf-8')

    def dccn_master(self, campaign_topic_map, geo_map, partner_map):
        df = self.df_dl
        month = self.s_month.split('-')
        df['Month of FiscalYear'] = self.s_month
        df['Month'] = month[1]

        blanks = df[(df['partner_name'].isna()) & (df['campaign_impressions'] > 1000)]
        print(blanks[['partner_name', 'SubscriberId', 'campaign_impressions']])
        print('!!!!!!!!!!!!!!!!!!FILL IN THE dccn_factevent.tsv SHEET WITH MISSING PARTNER NAMES!!!!!!!!!!!!!!!!!!!')

        # This is temporary, but it doesn't hurt to leave
        df['partner_name'] = np.where(df['SubscriberId'] == '12355b71', 'Pegasus Computer Limited', df['partner_name'])
        df['registered_country'] = np.where(df['SubscriberId'] == '12355b71', 'Taiwan', df['registered_country'])
        df['registered_url'] = np.where(df['SubscriberId'] == '12355b71', 'https://shop.pegasus.hk/',
                                        df['registered_url'])

        df = df.loc[df['partner_name'].isnull() == False]

        df.loc[df['registered_country'].isnull(), ['registered_country']] = df['country']
        df.loc[df['country'] == 'USA', ['registered_country']] = 'United States'
        df.loc[df['registered_country'] == 'Russia', ['registered_country']] = 'Russian Federation'
        df.loc[df['registered_country'] == 'Korea, South', ['registered_country']] = 'South Korea'
        df.loc[df['registered_country'] == 'Russia', ['registered_country']] = 'South Korea'

        df = pd.merge(df, campaign_topic_map, left_on='campaignname', right_on='Campaign Name', how='left')
        df = pd.merge(df, geo_map, left_on='registered_country', right_on='Country Name', how='left')
        df = pd.merge(df, partner_map, left_on='partner_name', right_on='Partner (Pre-aggregated)', how='left')

        df.loc[df['campaignname'].str.startswith('ms62'), ['campaignname']] = df['True Name']
        df.loc[df['Partner Type'].isnull(), ['Partner Type']] = 'Unmapped'
        # 'campaignname': 'Campaign Name',
        df = df.rename(columns={'Year': 'FiscalYear', 'banner_size': 'Banner',
                                'partner_name': 'Partner Name', 'LanguageName': 'Locale Name',
                                'registered_country': 'Country Name', 'registered_url': 'Registration URL',
                                'campaign_impressions': 'Impressions', 'click_through': 'Click Throughs',
                                'contact_us': 'Contact Us', 'buy_now': 'Buy Now',
                                'click_within': 'Microsite Click-withins', 'MS Region': 'Region'})
        df['Contact Us Submit'] = 0
        df['Campaign Code'] = ''
        df['Change if 1'] = ''
        df['Partner Name (Aggregated)'] = ''

        df = df[
            ['FiscalYear', 'Month of FiscalYear', 'Month', 'Campaign Topic', 'Campaign Name', 'Banner', 'Partner Name',
             'Region', 'Country Name', 'Locale Name', 'Registration URL', 'Impressions', 'Click Throughs',
             'Contact Us', 'Buy Now', 'Contact Us Submit', 'Microsite Click-withins', 'Campaign Code',
             'Partner Type', 'Change if 1', 'Partner Name (Aggregated)']]

        print(list(df.columns))
        print(list(self.df_master.columns))
        df2 = pd.concat([self.df_master, df], ignore_index=True)

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/dccn_trend_detail_raw.csv', index=False, encoding='utf-8')
        df2.to_csv(f'{self.archive_path}dccn_trend_detail_raw.csv', index=False, encoding='utf-8')

        # Create 2nd report for PowerBI
        df3 = pd.read_csv('Master/DCCN_Master_Source.csv', low_memory=False)

        df['MNA Hierarchy'] = ''
        df['Country Stage'] = ''
        df['Status'] = ''
        df['Registered Date'] = ''
        df['WeekNum'] = ''
        df['MonthNum'] = int(month[0])
        df['Week Number'] = ''
        df['Week End Date'] = ''
        df['Total Downloads'] = ''

        df = df[['FiscalYear', 'Month of FiscalYear', 'Month', 'Campaign Topic', 'MNA Hierarchy', 'Campaign Name', 'Partner Name', 'Country Stage', 'Region', 'Country Name', 'Status', 'Registered Date', 'Partner Type', 'Locale Name', 'WeekNum', 'Impressions', 'MonthNum', 'Week Number', 'Week End Date', 'Click Throughs', 'Contact Us', 'Buy Now', 'Total Downloads', 'Microsite Click-withins']]
        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8')
        df3.to_csv(f'{self.archive_path}DCCN_Master_Source.csv', index=False, encoding='utf-8')


def month_full(mon):
    match mon:
        case 'Jan':
            return 'January'
        case 'Feb':
            return 'February'
        case 'Mar':
            return 'March'
        case 'Apr':
            return 'April'
        case 'May':
            return 'May'
        case 'Jun':
            return 'June'
        case 'Jul':
            return 'July'
        case 'Aug':
            return 'August'
        case 'Sep':
            return 'September'
        case 'Oct':
            return 'October'
        case 'Nov':
            return 'November'
        case 'December':
            return 'December'
