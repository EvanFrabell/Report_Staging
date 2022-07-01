import os

from datetime import date

from excel_transform import TransformMaster


def main():
    today = date.today()
    report_month = int(today.strftime("%m"))

    if report_month == 1:
        report_month = 12
    else:
        report_month -= 1

    working_directory = f'{report_month}-{today.strftime("%Y")}'
    print(f'WORKING DIRECTORY: /Downloads/{working_directory}')

    path = f'Downloads/{working_directory}'
    file_list = os.listdir(path)

    for file in file_list:
        print('Working on ' + file)
        download = f'{path}/{file}'
        stage = f'Stage/{file}'

        match file:
            case 'ecomm_fact.tsv':
                master = f'Master/ecomm_raw.csv'
                TransformMaster(download, stage, master, file_name=file).ecomm()
                break
            case 'epson_finder.tsv':
                master = f'Master/epson_raw.csv'
                TransformMaster(download, stage, master, file_name=file).epson()
                break


if __name__ == '__main__':
    main()
