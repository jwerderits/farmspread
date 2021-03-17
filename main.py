import pdb
import requests
import json
import pandas
import os
import datetime
import creds
# pandas.set_option('display.max_columns', 500)
# pandas.set_option('display.width', 1000)


class Scrape:
    def __init__(self):
        self.headers = creds.headers
        self.url = creds.url
        self.cwd = os.getcwd()

    def make_request(self, url):
        response = requests.get(url, headers=self.headers)
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

    def filter_events(self, events, date_min=None, date_max=None):
        relevant_events = []
        if date_max:
            for event in events:
                start = datetime.datetime.strptime(event['start_time'], "%Y-%m-%dT%H:%M:%S")
                if start <= date_max and start >= date_min:
                    relevant_events.append(event)
            return relevant_events
        return events

    def define_structure(self):
        self.fields = ['vendor.id', 'vendor.name', 'vendor.data.attended', 'vendor.data.sales.amount', 'vendor.data.sales.breakdown_totals',
                   'vendor.data.sales.invoice.status', 'vendor.data.sales.invoice.total', 'vendor.data.sales']

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
        final_results = pandas.DataFrame()

        raw_event_data = make_request(event_url)
        data = raw_event_data['stalls']

        df = pandas.json_normalize(data)
        df = df.fillna(0)
        df = df.convert_dtypes()
        start_time = raw_event_data['start_datetime']
        end_time = raw_event_data['end_datetime']
        df=df[self.fields]

        for stall in data:
            if stall['vendor']:
                if stall['vendor']['data']['sales']:
                    for x, currency in enumerate(stall['vendor']['data']['sales']['breakdown']):
                        currency_type = currency['currency']
                        amount = currency['amount']
                        vendor_id = int(stall['vendor']['id'])
                        values = {currency_type: amount, 'vendor.id':vendor_id}
                        all_currencies['vendor.id'] = [vendor_id]
                        all_currencies[currency_type] = [amount]

                    final_results = final_results.append(all_currencies)

        file_name = raw_event_data['market']
        final_results.to_csv(f'{cwd}/{file_name}.csv')

def main():
    farmspread = Scrape()
    market_list = farmspread.find_markets(farmspread.url)
    season_list = farmspread.find_seasons(market_list)
    event_url = farmspread.find_events(season_list)
    start = datetime.datetime.strptime("2020-01-01","%Y-%m-%d")
    end = datetime.datetime.strptime("2020-12-31","%Y-%m-%d")
    events_to_scrape = farmspread.filter_events(event_url, start, end)
    

if __name__ == '__main__':
    main()
