import os
from datetime import date
import pandas as pd

from excel_transform import TransformMaster


def special_month(month):
    match month:
        case 1:
            return "07-Jan"
        case 2:
            return "08-Feb"
        case 3:
            return "09-Mar"
        case 4:
            return "10-Apr"
        case 5:
            return "11-May"
        case 6:
            return "12-Jun"
        case 7:
            return "01-Jul"
        case 8:
            return "02-Aug"
        case 9:
            return "03-Sep"
        case 10:
            return "04-Oct"
        case 11:
            return "05-Nov"
        case 12:
            return "06-Dec"


def main():
    today = date.today()
    report_month = int(today.strftime("%m"))

    if report_month == 1:
        report_month = 12
    else:
        report_month -= 1

    working_directory = f'{report_month}-{today.strftime("%Y")}'
    print(f'WORKING DIRECTORY: /Downloads/{working_directory}')

    # Reference tables
    campaign_code_map = pd.read_csv('ReferenceTable/campaign_code_map.csv', low_memory=False)
    campaign_topic_map = pd.read_csv('ReferenceTable/campaign_topic_map.csv', low_memory=False)
    geo_map = pd.read_csv('ReferenceTable/geo_map.csv', low_memory=False)
    lcid_map = pd.read_csv('ReferenceTable/lcid_map.csv', low_memory=False)
    partner_map = pd.read_csv('ReferenceTable/partner_map.csv', low_memory=False)
    price_map = pd.read_csv('ReferenceTable/price_map.csv', low_memory=False)
    product_map = pd.read_csv('ReferenceTable/product_map.csv', low_memory=False)
    asset_code = pd.read_csv('ReferenceTable/asset_code.csv', low_memory=False)



    alphanum_month = special_month(report_month)

    path = f'Downloads/{working_directory}'
    file_list = os.listdir(path)

    for file in file_list:
        print('Working on ' + file)
        download = f'{path}/{file}'
        stage = f'Stage/{file}'

        match file:
            ########################ECOMMERCE#############################
            # case 'ecomm_fact.tsv':
            #     master = 'Master/ecomm_raw.csv'
            #     bi_path = 'PowerBI/PBI-eComm/eCOMM_source.csv'
            #     TransformMaster(download, stage, master, bi_path, alphanum_month, working_directory).ecom(partner_map, lcid_map, geo_map, price_map)
            # case 'ms_surface_recommends_logo.tsv':
            #     master = 'Master/surface_raw.csv'
            #     bi_path = 'PowerBI/PBI-eComm/SurfaceRecommends_source.xlsx'
            #     TransformMaster(download, stage, master, bi_path, alphanum_month, working_directory).surface(partner_map, product_map)
            # case 'xbox.tsv':
            #     master = 'Master/xbox_raw.csv'
            #     bi_path = 'PowerBI/PBI-eComm/xbox_data.xlsx'
            #     TransformMaster(download, stage, master, bi_path, alphanum_month, working_directory).xbox(product_map)
            ########################DCCN#############################
            # case 'dccn_asset.tsv':
            #     master = 'Master/dccn_simplified_asset_raw.csv'
            #     bi_path = 'PowerBI/PBI-DCCN/DCCN_Asset data_source.csv'
            #     TransformMaster(download, stage, master, bi_path, alphanum_month, working_directory).dccn_asset(campaign_topic_map, asset_code)
            case 'dccn_factevent.tsv':
                master = 'Master/dccn_trend_detail_raw.csv'
                bi_path = 'PowerBI/PBI-DCCN/DCCN_Master_Source.csv'
                TransformMaster(download, stage, master, bi_path, alphanum_month, working_directory).dccn_master(
                    campaign_topic_map, geo_map, partner_map)
            # case 'epson_finder.tsv':
            #     master = f'Master/epson_raw.csv'
            #     bi_path = 'Null'
            #     TransformMaster(download, stage, master, bi_path, alphanum_month, working_directory).epson()


if __name__ == '__main__':
    main()
