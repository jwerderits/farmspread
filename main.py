import pdb
import requests
import json
import pandas
import os
import datetime
import creds
import boto3
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)


class Scrape:
    def __init__(self):
        self.headers = creds.headers
        self.url = creds.url
        self.cwd = os.getcwd()
        self.bucket_name = 'farmspread-data'
        self.today = datetime.datetime.today()
        self.current_month = datetime.datetime(self.today.year, self.today.month, 1)

    def make_request(self, url):
        response = requests.get(url, headers=self.headers)
        print(f'hitting {url} with response code {response.status_code}')
        return json.loads(response.text)

    def find_markets(self, url):
        market_list = []
        market_response = self.make_request(url)
        for market in market_response:
            market_list.append(market['resource_uri'])
        return market_list

    def find_seasons(self, market_list):
        season_list = []
        for season_urls in market_list:
            season_response = self.make_request(season_urls)
            for season in season_response['seasons']:
                season_list.append(season['resource_uri'])
        return season_list

    def find_events(self, season_list):
        event_list = []
        for event_urls in season_list:
            event_response = self.make_request(event_urls)
            for event in event_response['events']:
                event_list.append({'event_url':event['resource_uri'],
                    'start_time': event['start_datetime']})
        return event_list

    def load_to_s3(self, file_name, file_path):
        s3_client = boto3.client('s3', aws_access_key_id=creds.aws_key, aws_secret_access_key=creds.aws_secret_key)
        print(f'loading file: {file_name} to bucket: {self.bucket_name}')
        s3_client.upload_file(file_path, self.bucket_name, file_name)

    def determine_date_range(self):
        start = self.current_month
        almost_last_day = start.replace(day=28) + datetime.timedelta(days=4)
        end = almost_last_day - datetime.timedelta(days=start.day)
        if end > self.today:
            end = self.today
        return start, end

    def filter_events(self, events, date_min=None, date_max=None):
        relevant_events = []
        if date_max:
            for event in events:
                start = datetime.datetime.strptime(event['start_time'], "%Y-%m-%dT%H:%M:%S")
                if start <= date_max and start >= date_min:
                    relevant_events.append(event['event_url'])
            return relevant_events
        return events

    def define_structure(self):
        self.fields = ['vendor.id', 'vendor.name', 'vendor.data.attended', 'vendor.data.sales.amount', 'vendor.data.sales.breakdown_totals',
                   'vendor.data.sales.invoice.status', 'vendor.data.sales.invoice.total']

    #overwrites csv utilizing a df to remove whitespace and special chars from headers
    def clean_headers(self, dataframe):
        rename_dict = {}
        print('removing whitespace and special characters from columns')
        for column in dataframe.columns:
            cleaned = column.replace('#', 'number')
            cleaned = cleaned.replace(' ', '_')
            cleaned = cleaned.replace('\t', '')
            cleaned = cleaned.replace('/', '_')
            cleaned = cleaned.replace('-', '_')
            cleaned = cleaned.replace('.', '_')
            final = cleaned.lower()
            rename_dict.update({column: final})
        dataframe.rename(columns=rename_dict, inplace=True)

    def parse_events(self, event_url):
        all_currencies = pandas.DataFrame()
        full_market = pandas.DataFrame()

        raw_event_data = self.make_request(event_url)
        data = raw_event_data['stalls']
        market_date = datetime.datetime.strptime(raw_event_data['start_datetime'],"%Y-%m-%dT%H:%M:%S")
        new_format = "%Y-%m-%d"
        market_date = market_date.strftime(new_format)
        df = pandas.json_normalize(data)
        df = df.fillna(0)
        df = df.convert_dtypes()
        self.define_structure()

        df=df[self.fields]
        for stall in data:
            if stall['vendor']:
                #at the row level
                if stall['vendor']['data']['sales']:
                    running_total = 0
                    reimbursements = 0
                    #pulls apart the sales breakdown to flatten json
                    for x, currency in enumerate(stall['vendor']['data']['sales']['breakdown']):
                        currency_type = currency['currency']
                        amount = currency['amount']
                        vendor_name = stall['vendor']['name']
                        # vendor_id = int(stall['vendor']['id'])
                        # all_currencies['vendor_id'] = [vendor_id]
                        all_currencies['vendor_name'] = [vendor_name]
                        all_currencies[currency_type] = [amount]
                        running_total = running_total + (amount or 0)
                        if currency_type not in ('Cash', 'Charge', 'Check'):
                            reimbursements = reimbursements + (amount or 0)
                    vendor_fee = round(running_total * 0.06, 2)
                    net = reimbursements - vendor_fee
                    if net > 0:
                        vendor_owes = 0
                    else:
                        vendor_owes = net
                    reported_sales = stall['vendor']['data']['sales']['amount']
                    checksum = (reported_sales or 0) - running_total
                    all_currencies['reported_sales'] =reported_sales
                    all_currencies['total_sales'] = running_total
                    all_currencies['checksum'] = checksum
                    all_currencies['vendor_fee'] = vendor_fee
                    all_currencies['reimbursements'] = reimbursements
                    all_currencies['reimbursement_fee'] = net
                    all_currencies['vendor_owes'] = vendor_owes

                    full_market = full_market.append(all_currencies)
        full_market.insert(1, 'market', raw_event_data['market'])
        full_market.insert(2, 'market_date', market_date)

        return full_market

    def do_the_thing(self):
            complete_df = pandas.DataFrame()
            market_list = self.find_markets(self.url)
            season_list = self.find_seasons(market_list)
            event_url = self.find_events(season_list)
            start, end = self.determine_date_range()
            events_to_scrape = self.filter_events(event_url, start, end)
            print(events_to_scrape)
            for event in events_to_scrape:
                event_df = self.parse_events(event)
                complete_df = complete_df.append(event_df)

            file_name = f'{self.current_month.date()}.csv'
            file_path = f'{self.cwd}/{file_name}'
            complete_df.to_csv(file_path, index=False)
            self.load_to_s3(file_name, file_path)

def main():

    farmspread = Scrape()
    farmspread.do_the_thing()


if __name__ == '__main__':
    main()
