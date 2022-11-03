import os
from datetime import date

import numpy as np
import pandas as pd

pd.options.display.max_columns = 500
pd.options.display.width = 1000


# pd.options.display.max_rows = None


class TransformMaster:
    def __init__(self, download, stage, master, power_bi, s_month, ms_date):
        self.download = download
        self.stage = stage
        self.master = master
        self.power_bi = power_bi
        self.df_dl = pd.read_csv(self.download, '\t', low_memory=False)
        self.df_master = pd.read_csv(self.master, low_memory=False, encoding='utf-8-sig')
        self.s_month = s_month
        self.ms_month = int(ms_date.split("-")[0])
        self.ms_year = int(ms_date.split("-")[1])
        self.archive_path = f'Master/Archive/{ms_date}/'

        dir_exists = os.path.exists(self.archive_path)
        if not dir_exists:
            os.makedirs(self.archive_path)

    def ecom(self, p_map, l_map, g_map, price_map):
        df = self.df_dl

        df['partner'] = np.where(df['SubscriberId'] == '45fbdc77', 'Evetech', df['partner'])
        df['partner'] = np.where(df['SubscriberId'] == '6a349fab', 'SCGLOBAL', df['partner'])
        df['partner'] = np.where(df['SubscriberId'] == 'c375c05d', 'TECNOMEGA C.A.', df['partner'])

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
        # DANGEROUS!!!
        # df['Partner Name (Aggregated)'] = df['partner']
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
        df['Month'] = "=\"" + df['Month'] + "\""
        df2 = pd.concat([self.df_master, df])

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv(f'{self.archive_path}ecomm_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv('Workbench/Microsoft/ecomm_raw.csv', index=False, encoding='utf-8-sig')

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
        df4.to_csv(f'{self.archive_path}eCOMM_source.csv', index=False, encoding='utf-8-sig')
        df4.to_csv(self.power_bi, index=False, encoding='utf-8-sig')

    def epson(self):
        df = self.df_dl
        columns = ['month', 'yr', 'subscriberid', 'partner', 'Country', 'Action', 'Steps', 'Steps_selected',
                   'Conversion_MFR', 'Conversion_PN', 'EventCount']
        df = df[columns]

        df2 = pd.concat([self.df_master, df])

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv(f'{self.archive_path}epson_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'Workbench/Epson/epson_raw.csv', index=False, encoding='utf-8-sig')

    def surface(self, partner_map, product_map):
        df = self.df_dl
        df['year'] = self.ms_year
        df.insert(2, 'Mon', self.s_month, False)
        df = pd.merge(df, partner_map, left_on='partner_name', right_on='Partner (Pre-aggregated)', how='left')
        df = pd.merge(df, product_map, left_on='product_sku', right_on='PN', how='left')
        df = df.rename(columns={'Account Type': 'Account'})
        df = df[['month', 'year', 'Mon', 'program', 'access_country_name', 'requested_language', 'partner_name', 'skey',
                 'Account', 'mfr_name', 'product_sku', 'Product',
                 'Cross_Sell_SKU_Manufacturer', 'Cross_Sell_SKU_PN', 'Title', 'Price', 'impression', 'interaction',
                 'viewport', 'viewdetail', 'atc']]

        df['Mon'] = "'" + df['Mon']
        df2 = pd.concat([self.df_master, df])

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/surface_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}surface_raw.csv', index=False, encoding='utf-8-sig')
        # df2['Mon'] = df2['Mon'].str[2:8]
        df2.to_excel(self.power_bi, sheet_name='raw', index=False, header=True, encoding='utf-8-sig')

    def xbox(self, product_map):
        df = self.df_dl
        df['year'] = self.ms_year
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
        df2.to_csv('Workbench/Microsoft/xbox_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}xbox_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_excel(self.power_bi, sheet_name='raw', index=False, header=True, encoding='utf-8-sig')

    def dccn_asset(self, campaign_topic_map, asset_code):
        # FIX THE QUERY - THIS IS ASININE
        df = self.df_dl
        df['Year'] = self.ms_year
        month = self.s_month.split('-')
        long_month = month_full(month[1])
        df['Month'] = self.s_month
        df.insert(2, 'DisplayMonth', long_month, False)
        df['d'] = df['DownloadType'] + df['Asset']

        df = df.loc[df['SubscriberName'] != 'Dev Sandbox // DEV']
        # df = df[df['SubscriberName'].str.startswith('Dev Sandbox') == False]

        df['Partner Type'] = df['ParentTopicName']

        df = pd.merge(df, campaign_topic_map, left_on='Asset', right_on='Asset', how='left')
        df = pd.merge(df, asset_code, left_on='d', right_on='Concat', how='left')
        df = df.rename(
            columns={'Year': 'Fiscal Year', 'TopicName': 'Asset Sub', 'TopicSource': 'Partner Name',
                     'ParentTopicName': 'partnertype', 'Asset': 'Asset Name', 'EventCount': 'eventcount',
                     'Campaign Topic': 'Asset Topic', 'downloadtype': 'Asset Type',
                     'code': 'Asset Code', 'Level2Name': '2nd Level Sub-Topic'})

        df['Asset Sub-Topic'] = df['Campaign Name']
        df.insert(6, 'countryname', '', False)
        df.insert(6, 'countrystage', '', False)
        df.insert(6, 'region', '', False)

        df = df[['Month', 'Fiscal Year', 'DisplayMonth', 'eventcount', 'Partner Name', 'countryname', 'countrystage',
                 'region', 'partnertype', 'Asset Type', 'Asset Name', 'Partner Type', 'Asset Topic', 'Asset Sub',
                 'Asset Sub-Topic', '2nd Level Sub-Topic', 'Asset Code', 'd']]

        df2 = pd.concat([self.df_master, df], ignore_index=True)

        print(df2)
        # df2['Month'] = df2['Month'].apply(str)
        #
        # print(df2['Month'].dtypes)
        # print(df2)
        df2.loc[df2['Month'].str.len() < 6, ['Month']] = '0' + df2['Month']
        df2['Month'] = '="' + df2['Month'] + '"'

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/dccn_simplified_asset_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}dccn_simplified_asset_raw.csv', index=False, encoding='utf-8-sig')

        df = df.rename(columns={'Month': 'month', 'Fiscal Year': 'year'})
        df.insert(12, 'Campaign Sponsor', 'Microsoft', False)
        df['Month_Num'] = int(month[0])
        df = df[
            ['month', 'year', 'Month_Num', 'DisplayMonth', 'eventcount', 'Partner Name', 'countryname', 'countrystage',
             'region', 'partnertype', 'Asset Type', 'Asset Name', 'Campaign Sponsor', 'Partner Type', 'Asset Topic',
             'Asset Sub',
             'Asset Sub-Topic', '2nd Level Sub-Topic', 'Asset Code']]
        df3 = pd.read_csv('Master/DCCN_Asset data_source.csv', low_memory=False)
        df3 = pd.concat([df3, df], ignore_index=True)
        # df3 = df3.drop('Unnamed: 19', axis=1)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}DCCN_Asset data_source.csv', index=False, encoding='utf-8-sig')
        print(df3)

    def dccn_master(self, campaign_topic_map, geo_map, partner_map):
        df = self.df_dl
        month = self.s_month.split('-')
        df['Year'] = self.ms_year
        df['Month of FiscalYear'] = self.s_month
        df['Month'] = month[1]

        # This is temporary, but it doesn't hurt to leave
        df['partner_name'] = np.where(df['SubscriberId'] == '12355b71', 'Pegasus Computer Limited', df['partner_name'])
        df['registered_country'] = np.where(df['SubscriberId'] == '12355b71', 'Taiwan', df['registered_country'])
        df['registered_url'] = np.where(df['SubscriberId'] == '12355b71', 'https://shop.pegasus.hk/',
                                        df['registered_url'])

        blanks = df[(df['partner_name'].isna()) & (df['campaign_impressions'] > 1000)]
        print(blanks[['partner_name', 'SubscriberId', 'campaign_impressions']])
        print('!!!!!!!!!!!!!!!!!!FILL IN THE dccn_factevent.tsv SHEET WITH MISSING PARTNER NAMES!!!!!!!!!!!!!!!!!!!')

        df = df.loc[df['partner_name'].isnull() == False]

        df.loc[df['registered_country'].isnull(), ['registered_country']] = df['country']
        df.loc[df['country'] == 'USA', ['registered_country']] = 'United States'
        df.loc[df['registered_country'] == 'Russia', ['registered_country']] = 'Russian Federation'
        df.loc[df['registered_country'] == 'Korea, South', ['registered_country']] = 'Korea South'
        df.loc[df['registered_country'] == 'South Korea', ['registered_country']] = 'Korea South'

        # df.loc[df['Month of FiscalYear'].str.len() < 6, ['Month of FiscalYear']] = '0' + df['Month of FiscalYear']
        df['Month of FiscalYear'] = '="' + df['Month of FiscalYear'] + '"'

        df = pd.merge(df, campaign_topic_map, left_on='campaignname', right_on='Campaign Name', how='left')
        print(campaign_topic_map)
        df = pd.merge(df, geo_map, left_on='registered_country', right_on='Country Name', how='left')
        df = pd.merge(df, partner_map, left_on='partner_name', right_on='Partner (Pre-aggregated)', how='left')
        print(df.loc[df['partner_name'] == 'Weblink International Inc.'])

        # df.loc[df['campaignname'].str.startswith('ms62'), ['campaignname']] = df['True Name']
        # df.loc[df['True Name'] != 'xxx', ['campaignname']] = df['True Name']
        df['campaignname'] = np.where((df['True Name'] != 'xxx') & (df['True Name'].notnull()), df['True Name'],
                                      df['campaignname'])
        df.loc[df['Partner Type'].isnull(), ['Partner Type']] = 'Unmapped'

        print(df.loc[df['Campaign Name'] == 'ms6210282374400'])
        print(df.loc[df['partner_name'] == 'Weblink International Inc.'])

        df = df.drop(['Campaign Name'], axis=1)

        df = df.rename(columns={'Year': 'FiscalYear', 'campaignname': 'Campaign Name', 'banner_size': 'Banner',
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

        df2 = pd.concat([self.df_master, df], ignore_index=True)

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/dccn_trend_detail_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}dccn_trend_detail_raw.csv', index=False, encoding='utf-8-sig')

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

        df = df[['FiscalYear', 'Month of FiscalYear', 'Month', 'Campaign Topic', 'MNA Hierarchy', 'Campaign Name',
                 'Partner Name', 'Country Stage', 'Region', 'Country Name', 'Status', 'Registered Date', 'Partner Type',
                 'Locale Name', 'WeekNum', 'Impressions', 'MonthNum', 'Week Number', 'Week End Date', 'Click Throughs',
                 'Contact Us', 'Buy Now', 'Total Downloads', 'Microsite Click-withins']]

        df['Month of FiscalYear'] = df['Month of FiscalYear'].str.replace('=', '')
        df['Month of FiscalYear'] = df['Month of FiscalYear'].str.replace('"', '')

        df3 = pd.concat([df3, df], ignore_index=True)

        df3.loc[df3['Country Name'] == 'Russia', ['Country Name']] = 'Russian Federation'
        df3.loc[df3['Country Name'] == 'Korea, South', ['Country Name']] = 'Korea South'
        df3.loc[df3['Country Name'] == 'South Korea', ['Country Name']] = 'Korea South'

        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}DCCN_Master_Source.csv', index=False, encoding='utf-8-sig')

    def dccn_partner(self, geo_map):
        pass

    def dccn_gated(self):
        df = self.df_dl
        df['Year'] = self.ms_year
        month = self.s_month.split('-')
        df.insert(2, 'Month2', month[1], False)
        df['Month'] = month[0]

        df = df.rename(columns={'Month': 'month num', 'Year': 'Fiscal Year', 'Month2': 'Month', 'TopicId': 'topic_id',
                                'TopicName': 'topic_name',
                                'country': 'access_country_name', 'LanguageName': 'requested_language'})
        df = df[
            ['month num', 'Fiscal Year', 'Month', 'topic_id', 'topic_name', 'access_country_name', 'requested_language',
             'partner_name', 'download']]

        df['topic_id'] = df['topic_id'].str.lstrip('node:ms:')

        df2 = pd.concat([self.df_master, df], ignore_index=True)

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/dccn_gated_content_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}dccn_gated_content_raw.csv', index=False, encoding='utf-8-sig')

        df = df.rename(columns={'Fiscal Year': 'fiscal year'})

        df3 = pd.read_csv('Master/Gated_Content_Topic.csv', low_memory=False)
        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}Gated_Content_Topic.csv', index=False, encoding='utf-8-sig')

    def ar_master(self, geo_map, product_map):
        df = self.df_dl

        df['year'] = self.ms_year

        df = df.drop(['inline_content_impressions'], axis=1)
        df = df.drop(['viewport'], axis=1)

        df = pd.merge(df, geo_map, left_on='access_country_name', right_on='Country Name', how='left')
        df = pd.merge(df, product_map, left_on='clean_mfr_pn', right_on='PN', how='left')

        df = df.rename(columns={'Product': 'Category', 'MS Region': 'Region', 'Model': 'Product', 'mobile': 'Mobile'})
        df = df[['month', 'year', 'mfr_name', 'clean_mfr_pn', 'supplied_mfg_pn', 'Category', 'Product', 'Region',
                 'access_country_name', 'requested_language', 'partner_name', 'ActionName', 'asset', 'Program', 'Block',
                 'Mobile', 'interaction_count', 'viewport_all']]

        df.loc[df['Category'].isnull(), ['Category']] = 'Other'
        df.loc[df['Product'].isnull(), ['Product']] = 'Unmapped'

        df = df.drop(df[df['partner_name'].str.startswith('1W', na=False)].index)

        df2 = pd.concat([self.df_master, df], ignore_index=True)

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ar_master_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ar_master_raw.csv', index=False, encoding='utf-8-sig')

        df = df.rename(columns={'month': 'monthnum'})
        df = df.drop(['Category'], axis=1)
        df = df.drop(['Product'], axis=1)
        df = df.drop(['Region'], axis=1)
        df.insert(2, 'Month', self.s_month, False)

        df3 = pd.read_csv('Master/MS_AR_Source.csv', low_memory=False)
        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}MS_AR_Source.csv', index=False, encoding='utf-8-sig')

    def ar_url(self):
        df = self.df_dl
        df = df[['pn', 'partner', 'Url']]

        df2 = pd.concat([self.df_master, df], ignore_index=True)

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ar_url_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ar_url_raw.csv', index=False, encoding='utf-8-sig')

        df3 = pd.read_csv('Master/AR_URL_Source.csv', low_memory=False)
        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}AR_URL_Source.csv', index=False, encoding='utf-8-sig')

    def ms_inline(self, partner_map, product_map, geo_map):
        df = self.df_dl
        month = self.s_month.split('-')

        df['year'] = self.ms_year
        df['monthcalendarnum'] = calendar_month(month[1])
        df['nnmmm'] = self.s_month
        df['monthtext'] = month[1]
        df['monthnum'] = int(month[0])

        df = pd.merge(df, partner_map, left_on='partner', right_on='Partner (Pre-aggregated)', how='left')
        df = pd.merge(df, product_map, left_on='pn', right_on='PN', how='left')
        df = pd.merge(df, geo_map, left_on='access_country_name', right_on='Country Name', how='left')

        df = df.rename(columns={'inline_content_impressions': 'Impressions', 'inline_content_viewport': 'Viewport',
                                'inline_content_interactions': 'Interactions',
                                'impression_w_interaction_all': 'Interacted', 'Product': 'Category',
                                'MS Region': 'Region'})
        df = df[['year', 'monthcalendarnum', 'nnmmm', 'monthnum', 'monthtext', 'mfr_name', 'pn', 'partner', 'program',
                 'access_country_name', 'requested_language', 'Impressions', 'Viewport', 'Interactions', 'Interacted',
                 'Account Type', 'Model', 'Category', 'Region']]

        # df.loc[df['nnmmm'].str.len() < 6, ['nnmmm']] = '0' + df['nnmmm']
        # df['nnmmm'] = '="' + df['nnmmm'] + '"'

        df2 = pd.concat([self.df_master, df], ignore_index=True)

        # df2.loc[df2['nnmmm'].str.len() < 6, ['nnmmm']] = '0' + df2['nnmmm']
        # df2['nnmmm'] = '="' + df2['nnmmm'] + '"'

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ms_inline_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ms_inline_raw.csv', index=False, encoding='utf-8-sig')

        df = df[['year', 'monthcalendarnum', 'nnmmm', 'monthnum', 'monthtext', 'mfr_name', 'pn', 'partner', 'program',
                 'access_country_name', 'requested_language', 'Impressions', 'Viewport', 'Interactions', 'Interacted']]

        df3 = pd.read_csv('Master/Inline_Source.csv', low_memory=False)
        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}Inline_Source.csv', index=False, encoding='utf-8-sig')

    def ms_w11(self, partner_map, product_map):
        df = self.df_dl

        df['Month'] = self.s_month
        df['year'] = self.ms_year

        df = pd.merge(df, partner_map, left_on='partner_name', right_on='Partner (Pre-aggregated)', how='left')
        df = pd.merge(df, product_map, left_on='product_sku', right_on='PN', how='left')

        # for 1000+ logos served with no partner name, search for partner name in browser by subscriberID
        blanks = df[(df['partner_name'].isna()) & (df['impression'] > 1000)]
        print(blanks[['partner_name', 'access_country_name', 'mfr_name', 'product_sku', 'impression']])
        print('!!!!!!!!!!!!!!!!!!FILL IN THE ms_w11.tsv SHEET WITH MISSING PARTNERS!!!!!!!!!!!!!!!!!!!')
        df = df[df['partner_name'].isna() == False]

        df = df.rename(columns={'month': 'month_num', 'Account Type': 'Account'})
        df = df[['month_num', 'year', 'Month', 'program', 'access_country_name', 'requested_language', 'partner_name',
                 'mfr_name', 'product_sku', 'ActionContext', 'Account', 'Product', 'impression', 'interaction',
                 'viewport', 'Hover_impression', 'click', 'tab_navitation', 'feature_click']]

        df['Month'] = "'" + df['Month']

        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ms_w11_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ms_w11_raw.csv', index=False, encoding='utf-8-sig')

        df2 = df2[['month_num', 'year', 'Month', 'program', 'access_country_name', 'requested_language', 'partner_name',
                   'mfr_name', 'product_sku', 'ActionContext', 'Account', 'impression', 'interaction', 'viewport',
                   'Hover_impression', 'click', 'tab_navitation', 'feature_click']]

        # df2.loc[df['Month'].str.len() < 6, ['Month']] = '0' + df['Month']
        # df2['Month'] = '="' + df['Month'] + '"'

        # df3 = pd.read_csv('Master/ms_w11_raw.csv', low_memory=False)
        # df3 = pd.concat([df3, df], ignore_index=True)
        df2.to_csv(self.power_bi, index=False, encoding='utf-8-sig')

    def ms_cnet(self):
        df = self.df_dl
        df['year'] = self.ms_year
        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ms_cnet_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ms_cnet_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(self.power_bi, index=False, encoding='utf-8-sig')

    def ms_store(self):
        df = self.df_dl
        df['year'] = self.ms_year
        df = df.rename(columns={'inline_content_impressions': 'review_impressions'})
        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ms_store_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ms_store_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(self.power_bi, index=False, encoding='utf-8-sig')

    def ms_sis(self):
        df = self.df_dl
        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ms_sis_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ms_sis_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(self.power_bi, index=False, encoding='utf-8-sig')

    def complimentary_inject(self):
        pass
        # This is probably a worthless file in PowerBI - Don't bother
        # df = self.df_dl
        # df.insert(0, 'year', year, False)
        # df.insert(0, 'month', year, False)

    def ficon(self):
        df = self.df_dl
        # df['year'] = self.ms_year
        df = df.drop(['mobile'], axis=1)

        df2 = pd.concat([self.df_master, df], ignore_index=True)
        # df2 = df2.drop(['Column1'], axis=1)

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Microsoft/ficon_master_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}ficon_master_raw.csv', index=False, encoding='utf-8-sig')

    def asus_qbr(self, asus_map):
        df = self.df_dl

        month = self.s_month.split('-')
        mon = calendar_month(month[1])
        if mon < 4:
            quarter = 'Q1'
        elif mon >= 4 & mon <= 6:
            quarter = 'Q2'
        elif mon > 9:
            quarter = 'Q4'
        else:
            quarter = 'Q3'

        df['Quarter'] = quarter
        df = df[df['partner'].str.startswith('1World') == False]
        df = df[df['access_country_name'].isin(['USA', 'Canada'])]
        print(df)
        df = pd.merge(df, asus_map, left_on='pn', right_on='MPN', how='left')
        print(df)

        df = df.rename(columns={'Business unit': 'Business Unit', 'Model #': 'NewPN'})
        df = df[['Year', 'Month', 'Quarter', 'mfr_name', 'partner', 'pn', 'access_country_name', 'requested_language',
                 'program', 'Labels', 'Supcean', 'NewPN', 'Category', 'Business Unit', 'ProductId',
                 'inline_content_impressions', 'inline_content_viewport', 'inline_content_interactions',
                 'interacted_inline', 'video_play', 'feature_zoom', 'feature_hover', 'gallery_zoom', 'gallery_hover',
                 'hotspot_interactions', 'interactions_360', 'comptable_interactions']]

        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/Other/asus_qbr_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}asus_qbr_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(self.power_bi, index=False, encoding='utf-8-sig')

    def hp_inject(self):
        df = self.df_dl
        df['Year'] = self.ms_year
        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/HPLenovoDEllXIS/hp_injection_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}hp_injection_raw.csv', index=False, encoding='utf-8-sig')

        df3 = pd.read_csv('Master/HPMS_Injection_data_append_new.csv', low_memory=False)
        df = df.rename(columns={'Month': 'MonthNum', 'Inj': 'inj'})
        df.insert(2, 'Month', self.s_month, False)

        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}HPMS_Injection_data_append_new.csv', index=False, encoding='utf-8-sig')

    def dell_inject(self):
        df = self.df_dl

        df['Year'] = self.ms_year
        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/HPLenovoDEllXIS/dell_injection_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}dell_injection_raw.csv', index=False, encoding='utf-8-sig')

        df3 = pd.read_csv('Master/DellMS_Injection_data.csv', low_memory=False)
        df = df.rename(columns={'Month': 'MonthNum', 'Inj': 'inj'})
        df.insert(2, 'Month', self.s_month, False)

        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}DellMS_Injection_data.csv', index=False, encoding='utf-8-sig')

    def lenovo_inject(self):
        df = self.df_dl

        df['Year'] = self.ms_year
        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/HPLenovoDEllXIS/lenovo_injection_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}lenovo_injection_raw.csv', index=False, encoding='utf-8-sig')

        df3 = pd.read_csv('Master/LenovoMS_Injection_data.csv', low_memory=False)
        df = df.rename(columns={'Month': 'MonthNum', 'Inj': 'inj'})
        df.insert(2, 'Month', self.s_month, False)

        df3 = pd.concat([df3, df], ignore_index=True)
        df3.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        df3.to_csv(f'{self.archive_path}LenovoMS_Injection_data.csv', index=False, encoding='utf-8-sig')

    def xis_inject(self, partner_map, product_map):
        df = self.df_dl
        df['year'] = self.ms_year
        df['mm'] = self.s_month

        df = pd.merge(df, partner_map, left_on='partner', right_on='Partner (Pre-aggregated)', how='left')
        df = pd.merge(df, product_map, left_on='pn', right_on='PN', how='left')

        df = df.drop(['model'], axis=1)
        df = df.rename(columns={'Model': 'model', 'Account Type': 'Account'})
        df = df[['year', 'month', 'mm', 'mfr_name', 'pn', 'model', 'partner', 'Account', 'access_country_name',
                 'requested_language', 'inline_content_impressions', 'inline_content_viewport',
                 'inline_content_interactions', 'Url']]

        df.loc[df['mm'].str.len() < 6, ['mm']] = '0' + df['mm']
        df['mm'] = '="' + df['mm'] + '"'
        df = df[df['partner'].str.startswith('1World') == False]

        df2 = pd.concat([self.df_master, df], ignore_index=True)
        df.to_csv(self.stage, sep='\t', index=False)

        df2 = df2[df2['partner'].str.startswith('1World') == False]

        df2.to_csv('Workbench/HPLenovoDEllXIS/xis_injection_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}xis_injection_raw.csv', index=False, encoding='utf-8-sig')

        df2['model'] = ''
        df2 = df2.drop(['Account'], axis=1)
        df2['mm'] = df2['mm'].str.replace('=', '')
        df2['mm'] = df2['mm'].str.replace('"', '')
        # df3 = pd.read_csv('Master/xis_injection_raw.csv', low_memory=False)
        # df3 = pd.concat([df3, df], ignore_index=True)
        df2.to_csv(self.power_bi, index=False, encoding='utf-8-sig')
        # df2.to_csv(f'{self.archive_path}xis_injection_raw.csv', index=False, encoding='utf-8-sig')

    def url_inject(self):
        df = self.df_dl
        df['Year'] = self.ms_year

        df = df.rename(columns={'Inj': 'inj'})
        df = df[['mfr_name', 'Sponsor', 'ClearMfPn', 'access_country_name', 'requested_language', 'partner_name', 'Url',
                 'template', 'inj']]
        df2 = pd.concat([self.df_master, df], ignore_index=True)

        df2 = df2.drop_duplicates(['mfr_name', 'ClearMfPn'])

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv('Workbench/HPLenovoDEllXIS/hplenovodell_url_raw.csv', index=False, encoding='utf-8-sig')
        df2.to_csv(f'{self.archive_path}hplenovodell_url_raw.csv', index=False, encoding='utf-8-sig')


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


def calendar_month(mon):
    match mon:
        case 'Jan':
            return 1
        case 'Feb':
            return 2
        case 'Mar':
            return 3
        case 'Apr':
            return 4
        case 'May':
            return 5
        case 'Jun':
            return 6
        case 'Jul':
            return 7
        case 'Aug':
            return 8
        case 'Sep':
            return 9
        case 'Oct':
            return 10
        case 'Nov':
            return 11
        case 'December':
            return 12
