import openpyxl
import pandas as pd

pd.options.display.max_columns = None


# pd.options.display.max_rows = None


class TransformMaster:
    def __init__(self, download, stage, master, file_name):
        self.download = download
        self.stage = stage
        self.master = master
        self.file_name = file_name.split('.')[0]
        self.df_dl = pd.read_csv(self.download, '\t', low_memory=False)
        self.df_master = pd.read_csv(self.master, low_memory=False)

    def ecomm(self):
        print(self.df_dl)
        df = self.df_dl.rename(
            columns={'FiscalYear': 'Fiscal Year', 'FiscalQuarter': 'Fiscal Quarter', 'PartnerType': 'Partner Type',
                     'CountryUser': 'User Country', 'CrossSell_Manufacturer': 'Cross Sell SKU Mfr',
                     'Level_1': '1 - Product Type', 'Level_2': '2 - Product Version',
                     'Level_3': '3 - License/Product Type', 'Level_4': '4 - License Count/Yrs',
                     'Logos_served': 'Logos Served', 'Hover_impression': 'Hover Impressions',
                     'Hover_rate': 'Hover Rate', 'View_details': 'View Details', 'Add_to_cart': 'Add to Cart',
                     'Add_to_cart_qty': 'Add to Cart Qty w/ Multiplier', 'Add_to_cart_qty_base': 'Add to Cart Qty',
                     'ActionContext': 'Action Context', 'SubscriberId': 'Subscriber ID', 'Lcid': 'LCID',
                     'partner': 'Partner (Pre-aggregated)'})

        df.insert(15, 'Partner Name (Aggregated)', '', False)
        df.insert(15, 'Account Type', '', False)
        df.insert(15, 'Unit Price', '', False)
        df.insert(15, 'Total Revenue', '', False)
        df.insert(15, 'Concat', '', False)
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

        df.to_csv(self.stage, sep='\t', index=False)

    def epson(self):
        df = self.df_dl
        columns = ['month', 'yr', 'subscriberid', 'partner', 'Country', 'Action', 'Steps', 'Steps_selected',
                   'Conversion_MFR', 'Conversion_PN', 'EventCount']
        df = df[columns]

        df2 = pd.concat([self.df_master, df])

        df.to_csv(self.stage, sep='\t', index=False)
        df2.to_csv(f'Workbench/Epson/epson_raw.csv', index=False)

