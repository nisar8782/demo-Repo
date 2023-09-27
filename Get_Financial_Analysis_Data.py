
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import time
import numpy as np
import threading

Financial_data_Json = []
class ScrapeThread(threading.Thread):
    def __init__(self, url, sector, country, company, Exchange,page,ticker):
        threading.Thread.__init__(self)
        self.url = url
        self.sector=sector
        self.country = country
        self.company=company
        self.Exchange=Exchange
        self.page = page
        self.ticker=ticker
    
    def run(self):
        resp = requests.get(self.url)
        soup = BeautifulSoup(resp.content,'html.parser')
        row = {}
        row['Sector'] = self.sector
        row['Country'] = self.country
        row['Company Name'] = self.company
        row['Ticker'] = self.ticker
        row['Exchange'] = self.Exchange
        # if 'Austria' in self.country and 'Amundi S.A' in self.company:
        #     print('found: ',self.page, self.company)
        try:
            for finan_data in soup.find('div',{'class':'element element--list'}).find('ul').find_all('li'):
                Key = finan_data.find('small').text.strip()
                value = finan_data.find('span').text
                row[Key]=value
            Financial_data_Json.append(row)
            # print(row)
        except:print('Error: ',self.page, self.company)
def finacial_analysis(sectors, regions, alp,path_sector_file):
    df = path_sector_file
    sector_filter = df[df['sector'].isin(sectors)]
    super_sectors = json.loads(sector_filter['Market Watch Sectors'].to_json(orient='records'))
    # alp = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    # alp = ['A']
    for a in alp:
        page = 1
        threads = []
        while True:
            try:
                resp = requests.get(f'https://www.marketwatch.com/tools/markets/stocks/a-z/{a}/{page}')
                soup = BeautifulSoup(resp.content,'html.parser')
                if soup.find('table').find('tbody').find_all('tr') == []:
                    break
                for i in soup.find('table').find('tbody').find_all('tr'):
                    sector = i.find_all('td')[-1].text.strip()
                    country = i.find_all('td')[1].text.strip()
                    if sector in super_sectors and country in regions:
                        link = "https://www.marketwatch.com" + i.find_all('td')[0].find('a').get('href')
                        company = i.find_all('td')[0].find('a').text.split('(')[0].strip()
                        ticker = i.find_all('td')[0].find('a').text.split('(')[1].replace(')','').strip()
                        Exchange = i.find_all('td')[2].text.strip()
                        t = ScrapeThread(link, sector, country, company, Exchange,page,ticker)
                        t.start()
                        threads.append(t)
                            # print(Key,value)
                # break
            except:pass
            page = page+1
            time.sleep(0.5)
        for t in threads:
            t.join()
    def convert_to_number(x):
        if isinstance(x, str):
            if x[-1] == 'K':
                return float(x[:-1]) * 1
            elif x[-1] == 'M':
                return float(x[:-1]) * 1000
            elif x[-1] == 'B':
                return float(x[:-1]) * 1000000
            elif x[-1] == 'T':
                return float(x[:-1]) * 1000000000
        return x
    def convert_to_number_with_sign(x):
        if isinstance(x, str):
            try:
                float(x[1:-1])
                sign = x[0]
                x = x[1:]
            except:
                try:
                    float(x[2:-1])
                    sign = x[0:2]
                    x = x[2:]
                except:
                    float(x[3:-1])
                    sign = x[0:3]
                    x = x[3:]
            if x[-1] == 'K':
                return sign+str(float(x[:-1]) * 1)
            elif x[-1] == 'M':
                return sign+str(float(x[:-1]) * 1000)
            elif x[-1] == 'B':
                return sign+str(float(x[:-1]) * 1000000)
            elif x[-1] == 'T':
                return sign+str(float(x[:-1]) * 1000000000)
        return x
        
    Financial_df = pd.DataFrame(Financial_data_Json)
    Financial_df1 = Financial_df[['Country','Sector','Exchange','Company Name','Ticker','Market Cap','Beta','Average Volume','Shares Outstanding','P/E Ratio','Yield','Public Float']].copy()
    Financial_df1.replace('N/A', np.nan, inplace=True)
    # Financial_df1.replace('NA', np.nan, inplace=True) ###############
    Financial_df1['Average Volume'] = Financial_df1['Average Volume'].apply(convert_to_number)
    Financial_df1['Market Cap'] = Financial_df1['Market Cap'].apply(convert_to_number_with_sign)
    Financial_df1['Shares Outstanding'] = Financial_df1['Shares Outstanding'].apply(convert_to_number)
    Financial_df1['Public Float'] = Financial_df1['Public Float'].apply(convert_to_number)
    Financial_df1['Beta'] = Financial_df1['Beta'].astype(float)
    Financial_df1['Average Volume'] = Financial_df1['Average Volume'].astype(float)
    # Financial_df1['Average Volume'] = Financial_df1['Average Volume'].astype(int)
    # Financial_df1['Shares Outstanding'] = pd.to_numeric(Financial_df1['Shares Outstanding'], errors='coerce').fillna(0).astype(int)
    Financial_df1['Shares Outstanding'] = Financial_df1['Shares Outstanding'].astype(float)
    # Financial_df1['Shares Outstanding'] = Financial_df1['Shares Outstanding'].astype(int)
    Financial_df1['P/E Ratio'] = Financial_df1['P/E Ratio'].str.replace(',', '').astype(float)
    Financial_df1['Public Float'] = Financial_df1['Public Float'].astype(float)
    Financial_df1.rename(columns={'Yield': 'Yield(%)'}, inplace=True)
    Financial_df1['Yield(%)'] = Financial_df1['Yield(%)'].str[:-1]
    Financial_df1['Yield(%)'] = Financial_df1['Yield(%)'].astype(float)
    Financial_df1 = Financial_df1.drop_duplicates(keep='first')
    return Financial_df1

# run to get the currenciese values in USD
# Covert the different currencies into USD
def Convert_to_USD(Financial_df,target_currency):
    countries_and_currencies = [
    {'Abbreviation': 'UAE','World Countries': 'United Arab Emirates','Currency': 'Dirham','Symbol': 'AED','Sign': 'د.إ.\u200f', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AFG','World Countries': 'Afghanistan','Currency': 'Afghani','Symbol': 'AFN','Sign': '؋', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AL','World Countries': 'Albania','Currency': 'Albanian Lek','Symbol': 'ALL','Sign': 'Lek', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ARM','World Countries': 'Armenia','Currency': 'Dram','Symbol': 'AMD','Sign': '֏', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CW','World Countries': 'Curaçao','Currency': 'Netherlands Antillean Guilder','Symbol': 'ANG','Sign': 'ƒ', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AGO','World Countries': 'Angola','Currency': 'Kwanza','Symbol': 'AOA','Sign': 'Kz', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AR','World Countries': 'Argentina','Currency': 'Argentine Peso','Symbol': 'ARS','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AU','World Countries': 'Australia','Currency': 'Australian Dollar','Symbol': 'AUD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ABW','World Countries': 'Aruba','Currency': 'Florin','Symbol': 'AWG','Sign': 'ƒ', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AZE','World Countries': 'Azerbaijan','Currency': 'Manat','Symbol': 'AZN','Sign': '₼', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BA','World Countries': 'Bosnia and Herzegovina','Currency': 'Convertible Mark','Symbol': 'BAM','Sign': 'KM', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BRB','World Countries': 'Barbados','Currency': 'Dollar','Symbol': 'BBD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BGD','World Countries': 'Bangladesh','Currency': 'Taka','Symbol': 'BDT','Sign': '৳', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BG','World Countries': 'Bulgaria','Currency': 'Bulgarian Lev','Symbol': 'BGN','Sign': 'лв', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BH','World Countries': 'Bahrain','Currency': 'Bahraini Dinar','Symbol': 'BHD','Sign': '.د.ب', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BDI','World Countries': 'Burundi','Currency': 'Franc','Symbol': 'BIF','Sign': 'FBu', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BM','World Countries': 'Bermuda','Currency': 'Bermudian Dollar','Symbol': 'BMD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BRN','World Countries': 'Brunei','Currency': 'Dollar','Symbol': 'BND','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BOL','World Countries': 'Bolivia','Currency': 'Boliviano','Symbol': 'BOB','Sign': 'Bs.', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BR','World Countries': 'Brazil','Currency': 'Brazilian Real','Symbol': 'BRL','Sign': 'R$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BS','World Countries': 'Bahamas','Currency': 'Bahamian Dollar','Symbol': 'BSD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BTN','World Countries': 'Bhutan','Currency': 'Ngultrum','Symbol': 'BTN','Sign': 'Nu.', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BWA','World Countries': 'Botswana','Currency': 'Pula','Symbol': 'BWP','Sign': 'P', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BY','World Countries': 'Belarus','Currency': 'Belarusian Ruble','Symbol': 'BYN','Sign': 'Br', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BLZ','World Countries': 'Belize','Currency': 'Dollar','Symbol': 'BZD','Sign': 'BZ$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CA','World Countries': 'Canada','Currency': 'Canadian Dollar','Symbol': 'CAD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'COD','World Countries': 'Democratic Republic of Congo','Currency': 'Franc','Symbol': 'CDF','Sign': 'FC', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LI','World Countries': 'Liechtenstein','Currency': 'Swiss Franc','Symbol': 'CHF','Sign': 'Fr', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CH','World Countries': 'Switzerland','Currency': 'Swiss Franc','Symbol': 'CHF','Sign': 'Fr', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CL','World Countries': 'Chile','Currency': 'Chilean Peso','Symbol': 'CLP','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CN','World Countries': 'China','Currency': 'Chinese Yuan','Symbol': 'CNY','Sign': '¥', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CO','World Countries': 'Colombia','Currency': 'Colombian Peso','Symbol': 'COP','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CRI','World Countries': 'Costa Rica','Currency': 'Colon','Symbol': 'CRC','Sign': '₡', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CUB','World Countries': 'Cuba','Currency': 'Peso','Symbol': 'CUP','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CPV','World Countries': 'Cape Verde','Currency': 'Escudo','Symbol': 'CVE','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CZ','World Countries': 'Czech Republic','Currency': 'Czech Koruna','Symbol': 'CZK','Sign': 'Kč', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'DJI','World Countries': 'Djibouti','Currency': 'Franc','Symbol': 'DJF','Sign': 'Fdj', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'DK','World Countries': 'Denmark','Currency': 'Danish Krone','Symbol': 'DKK','Sign': 'kr', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'DO','World Countries': 'Dominican Republic','Currency': 'Dominican Peso','Symbol': 'DOP','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'DZA','World Countries': 'Algeria','Currency': 'Dinar','Symbol': 'DZD','Sign': 'د.ج.', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'EG','World Countries': 'Egypt','Currency': 'Egyptian Pound','Symbol': 'EGP','Sign': 'ج.م', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ERI','World Countries': 'Eritrea','Currency': 'Nakfa','Symbol': 'ERN','Sign': 'Nfk', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ETH','World Countries': 'Ethiopia','Currency': 'Birr','Symbol': 'ETB','Sign': 'Br', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AD','World Countries': 'Andorra','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'AT','World Countries': 'Austria','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'BE','World Countries': 'Belgium','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CY','World Countries': 'Cyprus','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'EE','World Countries': 'Estonia','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'FI','World Countries': 'Finland','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'FR','World Countries': 'France','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'DE','World Countries': 'Germany','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GR','World Countries': 'Greece','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IE','World Countries': 'Ireland','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IT','World Countries': 'Italy','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'XK','World Countries': 'Kosovo','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LV','World Countries': 'Latvia','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LT','World Countries': 'Lithuania','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LU','World Countries': 'Luxembourg','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MT','World Countries': 'Malta','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MC','World Countries': 'Monaco','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ME','World Countries': 'Montenegro','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'NL','World Countries': 'Netherlands','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PT','World Countries': 'Portugal','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SM','World Countries': 'San Marino','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SK','World Countries': 'Slovakia','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SI','World Countries': 'Slovenia','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ES','World Countries': 'Spain','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'VA','World Countries': 'Vatican City','Currency': 'Euro','Symbol': 'EUR','Sign': '€', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'FJI','World Countries': 'Fiji','Currency': 'Dollar','Symbol': 'FJD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'FLK','World Countries': 'Falkland Islands','Currency': 'Pound','Symbol': 'FKP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'FRO','World Countries': 'Faroe Islands','Currency': 'Krone','Symbol': 'FOK','Sign': 'kr', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IM','World Countries': 'Isle of Man','Currency': 'British Pound','Symbol': 'IMP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'U.K.','World Countries': 'United Kingdom','Currency': 'Pound Sterling','Symbol': 'GBP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GEO','World Countries': 'Georgia','Currency': 'Lari','Symbol': 'GEL','Sign': '₾', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GGY','World Countries': 'Guernsey','Currency': 'Pound','Symbol': 'GGP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GHA','World Countries': 'Ghana','Currency': 'Cedi','Symbol': 'GHS','Sign': '₵', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GI','World Countries': 'Gibraltar','Currency': 'Gibraltar Pound','Symbol': 'GIP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GMB','World Countries': 'Gambia','Currency': 'Dalasi','Symbol': 'GMD','Sign': 'D', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GIN','World Countries': 'Guinea','Currency': 'Franc','Symbol': 'GNF','Sign': 'FG', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GTM','World Countries': 'Guatemala','Currency': 'Quetzal','Symbol': 'GTQ','Sign': 'Q', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'GUY','World Countries': 'Guyana','Currency': 'Dollar','Symbol': 'GYD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'HK','World Countries': 'Hong Kong','Currency': 'Hong Kong Dollar','Symbol': 'HKD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'HND','World Countries': 'Honduras','Currency': 'Lempira','Symbol': 'HNL','Sign': 'L', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'HR','World Countries': 'Croatia','Currency': 'Croatian Kuna','Symbol': 'HRK','Sign': 'kn', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'HTI','World Countries': 'Haiti','Currency': 'Gourde','Symbol': 'HTG','Sign': 'G', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'HU','World Countries': 'Hungary','Currency': 'Hungarian Forint','Symbol': 'HUF','Sign': 'Ft', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ID','World Countries': 'Indonesia','Currency': 'Indonesian Rupiah','Symbol': 'IDR','Sign': 'Rp', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IL','World Countries': 'Israel','Currency': 'Israeli Shekel','Symbol': 'ILS','Sign': '₪', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IN','World Countries': 'India','Currency': 'Indian Rupee','Symbol': 'INR','Sign': '₹', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IRQ','World Countries': 'Iraq','Currency': 'Dinar','Symbol': 'IQD','Sign': 'ع.د.', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IRN','World Countries': 'Iran','Currency': 'Rial','Symbol': 'IRR','Sign': '﷼', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'IS','World Countries': 'Iceland','Currency': 'Icelandic Krona','Symbol': 'ISK','Sign': 'kr', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'JEY','World Countries': 'Jersey','Currency': 'Pound','Symbol': 'JEP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'JAM','World Countries': 'Jamaica','Currency': 'Dollar','Symbol': 'JMD','Sign': 'J$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'JO','World Countries': 'Jordan','Currency': 'Jordanian Dinar','Symbol': 'JOD','Sign': 'د.ا', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'JP','World Countries': 'Japan','Currency': 'Japanese Yen','Symbol': 'JPY','Sign': '¥', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KEN','World Countries': 'Kenya','Currency': 'Shilling','Symbol': 'KES','Sign': 'KSh', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KGZ','World Countries': 'Kyrgyzstan','Currency': 'Som','Symbol': 'KGS','Sign': 'som', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KHM','World Countries': 'Cambodia','Currency': 'Riel','Symbol': 'KHR','Sign': '៛', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KIR','World Countries': 'Kiribati','Currency': 'Dollar','Symbol': 'KID','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'COM','World Countries': 'Comoros','Currency': 'Franc','Symbol': 'KMF','Sign': 'CF', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KR','World Countries': 'South Korea','Currency': 'South Korean Won','Symbol': 'KRW','Sign': '₩', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KW','World Countries': 'Kuwait','Currency': 'Kuwaiti Dinar','Symbol': 'KWD','Sign': 'د.ك', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KY','World Countries': 'Cayman Islands','Currency': 'Cayman Islands Dollar','Symbol': 'KYD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'KZ','World Countries': 'Kazakhstan','Currency': 'Kazakhstani Tenge','Symbol': 'KZT','Sign': '₸', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LAO','World Countries': 'Laos','Currency': 'Kip','Symbol': 'LAK','Sign': '₭', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LB','World Countries': 'Lebanon','Currency': 'Lebanese Pound','Symbol': 'LBP','Sign': 'ل.ل', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LK','World Countries': 'Sri Lanka','Currency': 'Sri Lankan Rupee','Symbol': 'LKR','Sign': 'Rs', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LBR','World Countries': 'Liberia','Currency': 'Dollar','Symbol': 'LRD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LSO','World Countries': 'Lesotho','Currency': 'Loti','Symbol': 'LSL','Sign': 'L', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'LBY','World Countries': 'Libya','Currency': 'Dinar','Symbol': 'LYD','Sign': 'ل.د.', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MA','World Countries': 'Morocco','Currency': 'Moroccan Dirham','Symbol': 'MAD','Sign': 'د.م', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MD','World Countries': 'Moldova','Currency': 'Moldovan Leu','Symbol': 'MDL','Sign': 'lei', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MDG','World Countries': 'Madagascar','Currency': 'Ariary','Symbol': 'MGA','Sign': 'Ar', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MK','World Countries': 'North Macedonia','Currency': 'Macedonian Denar','Symbol': 'MKD','Sign': 'ден', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MMR','World Countries': 'Myanmar (Burma)','Currency': 'Kyat','Symbol': 'MMK','Sign': 'K', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MNG','World Countries': 'Mongolia','Currency': 'Tughrik','Symbol': 'MNT','Sign': '₮', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MAC','World Countries': 'Macau','Currency': 'Pataca','Symbol': 'MOP','Sign': 'MOP$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MRT','World Countries': 'Mauritania','Currency': 'Ouguiya','Symbol': 'MRU','Sign': 'UM', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MU','World Countries': 'Mauritius','Currency': 'Mauritian Rupee','Symbol': 'MUR','Sign': '₨', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MDV','World Countries': 'Maldives','Currency': 'Rufiyaa','Symbol': 'MVR','Sign': 'Rf', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MWI','World Countries': 'Malawi','Currency': 'Kwacha','Symbol': 'MWK','Sign': 'MK', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MX','World Countries': 'Mexico','Currency': 'Mexican Peso','Symbol': 'MXN','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MY','World Countries': 'Malaysia','Currency': 'Malaysian Ringgit','Symbol': 'MYR','Sign': 'RM', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'MOZ','World Countries': 'Mozambique','Currency': 'Metical','Symbol': 'MZN','Sign': 'MT', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': None,'World Countries': 'Namibia','Currency': 'Namibian Dollar','Symbol': 'NAD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'NGA','World Countries': 'Nigeria','Currency': 'Naira','Symbol': 'NGN','Sign': '₦', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'NIC','World Countries': 'Nicaragua','Currency': 'Cordoba','Symbol': 'NIO','Sign': 'C$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'NO','World Countries': 'Norway','Currency': 'Norwegian Krone','Symbol': 'NOK','Sign': 'kr', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'NPL','World Countries': 'Nepal','Currency': 'Rupee','Symbol': 'NPR','Sign': 'रू', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': None,'World Countries': 'International Monetary Fund','Currency': 'Special Drawing Rights','Symbol': None,'Sign': 'XDR', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'NZ','World Countries': 'New Zealand','Currency': 'New Zealand Dollar','Symbol': 'NZD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'OM','World Countries': 'Oman','Currency': 'Omani Rial','Symbol': 'OMR','Sign': 'ر.ع', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PA','World Countries': 'Panama','Currency': 'Panamanian Balboa','Symbol': 'PAB','Sign': 'B/.', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PE','World Countries': 'Peru','Currency': 'Peruvian Sol','Symbol': 'PEN','Sign': 'S/', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PNG','World Countries': 'Papua New Guinea','Currency': 'Kina','Symbol': 'PGK','Sign': 'K', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PH','World Countries': 'Philippines','Currency': 'Philippine Peso','Symbol': 'PHP','Sign': '₱', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PK','World Countries': 'Pakistan','Currency': 'Pakistani Rupee','Symbol': 'PKR','Sign': '₨', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PL','World Countries': 'Poland','Currency': 'Polish Złoty','Symbol': 'PLN','Sign': 'zł', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'PY','World Countries': 'Paraguay','Currency': 'Paraguayan Guarani','Symbol': 'PYG','Sign': '₲', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'QA','World Countries': 'Qatar','Currency': 'Qatari Riyal','Symbol': 'QAR','Sign': 'ر.ق', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'RO','World Countries': 'Romania','Currency': 'Romanian Leu','Symbol': 'RON','Sign': 'lei', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'RS','World Countries': 'Serbia','Currency': 'Serbian Dinar','Symbol': 'RSD','Sign': 'РСД', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'RU','World Countries': 'Russia','Currency': 'Russian Ruble','Symbol': 'RUB','Sign': '₽', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'RWA','World Countries': 'Rwanda','Currency': 'Franc','Symbol': 'RWF','Sign': 'FRw', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SAU','World Countries': 'Saudi Arabia','Currency': 'Riyal','Symbol': 'SAR','Sign': 'ر.س.', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SLB','World Countries': 'Solomon Islands','Currency': 'Dollar','Symbol': 'SBD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SYC','World Countries': 'Seychelles','Currency': 'Rupee','Symbol': 'SCR','Sign': '₨', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SDN','World Countries': 'Sudan','Currency': 'Pound','Symbol': 'SDG','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SE','World Countries': 'Sweden','Currency': 'Swedish Krona','Symbol': 'SEK','Sign': 'kr', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SGP','World Countries': 'Singapore','Currency': 'Dollar','Symbol': 'SGD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SHN','World Countries': 'Saint Helena','Currency': 'Pound','Symbol': 'SHP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SLE','World Countries': 'Sierra Leone','Currency': 'Leone','Symbol': 'SLE','Sign': 'Le', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SLE','World Countries': 'Somalia','Currency': 'Shilling','Symbol': 'SLL','Sign': 'S', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': None,'World Countries': 'Somaliland','Currency': 'Shilling','Symbol': 'SOS','Sign': 'S', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SUR','World Countries': 'Suriname','Currency': 'Dollar','Symbol': 'SRD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SSD','World Countries': 'South Sudan','Currency': 'Pound','Symbol': 'SSP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'STP','World Countries': 'São Tomé and Príncipe','Currency': 'Dobra','Symbol': 'STN','Sign': 'Db', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SYR','World Countries': 'Syria','Currency': 'Pound','Symbol': 'SYP','Sign': '£', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'SWZ','World Countries': 'Eswatini (Swaziland)','Currency': 'Lilangeni','Symbol': 'SZL','Sign': 'E', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TH','World Countries': 'Thailand','Currency': 'Thai Baht','Symbol': 'THB','Sign': '฿', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TJK','World Countries': 'Tajikistan','Currency': 'Somoni','Symbol': 'TJS','Sign': 'SM', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TKM','World Countries': 'Turkmenistan','Currency': 'Manat','Symbol': 'TMT','Sign': 'm', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TN','World Countries': 'Tunisia','Currency': 'Tunisian Dinar','Symbol': 'TND','Sign': 'د.ت', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TON','World Countries': 'Tonga','Currency': "Pa'anga",'Symbol': 'TOP','Sign': 'T$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TR','World Countries': 'Turkey','Currency': 'Turkish Lira','Symbol': 'TRY','Sign': '₺', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TTO','World Countries': 'Trinidad and Tobago','Currency': 'Dollar','Symbol': 'TTD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TUV','World Countries': 'Tuvalu','Currency': 'Dollar','Symbol': 'TVD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TW','World Countries': 'Taiwan','Currency': 'New Taiwan Dollar','Symbol': 'TWD','Sign': 'NT$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'TZA','World Countries': 'Tanzania','Currency': 'Shilling','Symbol': 'TZS','Sign': 'TSh', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'UA','World Countries': 'Ukraine','Currency': 'Ukrainian Hryvnia','Symbol': 'UAH','Sign': '₴', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'UGA','World Countries': 'Uganda','Currency': 'Shilling','Symbol': 'UGX','Sign': 'USh', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'VG','World Countries': 'British Virgin Islands','Currency': 'US Dollar','Symbol': 'USD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'EC','World Countries': 'Ecuador','Currency': 'US Dollar','Symbol': 'USD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'U.S.','World Countries': 'United States','Currency': 'US Dollar','Symbol': 'USD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'URY','World Countries': 'Uruguay','Currency': 'Peso','Symbol': 'UYU','Sign': '$U', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'UZB','World Countries': 'Uzbekistan','Currency': 'Som','Symbol': 'UZS','Sign': "so'm", 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'VE','World Countries': 'Venezuela','Currency': 'Venezuelan Bolivar','Symbol': 'VES','Sign': 'Bs.S', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'VNM','World Countries': 'Vietnam','Currency': 'Dong','Symbol': 'VND','Sign': '₫', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'VUT','World Countries': 'Vanuatu','Currency': 'Vatu','Symbol': 'VUV','Sign': 'VT', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'WS','World Countries': 'Western Samoa','Currency': 'Tala','Symbol': 'WST','Sign': 'T', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'CAF','World Countries': 'Central African CFA franc','Currency': 'Franc','Symbol': 'XAF','Sign': 'FCFA', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': None,'World Countries': 'East Caribbean Dollar','Currency': 'Dollar','Symbol': 'XCD','Sign': '$', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': None,'World Countries': 'West African CFA franc','Currency': 'Franc','Symbol': 'XOF','Sign': 'CFA', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': None,'World Countries': 'CFP Franc','Currency': 'Franc','Symbol': 'XPF','Sign': 'F', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'YEM','World Countries': 'Yemen','Currency': 'Rial','Symbol': 'YER','Sign': '﷼', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ZA','World Countries': 'South Africa','Currency': 'South African Rand','Symbol': 'ZAR','Sign': 'R', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ZMB','World Countries': 'Zambia','Currency': 'Kwacha','Symbol': 'ZMW','Sign': 'ZK', 'Currency target': None,'ExchRate': None},
    {'Abbreviation': 'ZWE','World Countries': 'Zimbabwe','Currency': 'Dollar','Symbol': 'ZWL','Sign': '$', 'Currency target': None,'ExchRate': None}
    ]

    
    countries_and_currencies = pd.DataFrame(countries_and_currencies)
    url = f'https://v6.exchangerate-api.com/v6/7d995b34c57271668afc65b7/latest/{target_currency}'
    # Making our request
    response = requests.get(url)
    data = response.json()
    # # convert to dataframe
    cdf_table = pd.DataFrame(data)
    cdf_table = cdf_table.reset_index()
    cdf = cdf_table.rename(columns={'index': 'Symbol', 'base_code': 'Currency target', 'conversion_rates':'ExchRate'})
    cdf = cdf[['Symbol', 'Currency target', 'ExchRate']]
    cdf1 = pd.merge(countries_and_currencies, cdf, how="left", left_on="Symbol", right_on="Symbol")
    cdf1 = cdf1.rename(columns={'ExchRate_y': 'ExchRate', 'Currency target_y': 'Currency target'})
    industry_and_currency_df = cdf1[['World Countries','Symbol','Sign','ExchRate','Currency target']].copy()
    Financial_df1 = Financial_df.copy()
    # industry_and_currency_df = pd.read_excel('industry_and_currency.xlsx')
    Financial_df1 = pd.merge(Financial_df1, industry_and_currency_df, how="left", left_on="Country", right_on="World Countries")
    # Convert the Market Cap values to US dollars
    def convert_to_usd(market_cap, currency, exchange_rate,currency_sign):
        if isinstance(market_cap, str):
            try:
                float(str(market_cap[1:]))
                sign = market_cap[0].replace('.','')
                market_cap = market_cap[1:]
            except:
                try:
                    float(str(market_cap[2:]))
                    sign = market_cap[0:2].replace('.','')
                    market_cap = market_cap[2:]
                except:
                    float(str(market_cap[3:]))
                    sign = market_cap[0:3].replace('.','')
                    market_cap = market_cap[3:]
            if currency == Financial_df1["Currency target"][0]:
                return market_cap
            elif currency_sign == sign:
                return float(market_cap) / exchange_rate
            else:
                return None
        return market_cap
    Financial_df1[f"Market Cap ({target_currency})"] = Financial_df1.apply(lambda row: convert_to_usd(row["Market Cap"], row["Symbol"], row["ExchRate"],row['Sign']), axis=1)
    Financial_df1 = Financial_df1[['Country','Sector','Exchange','Company Name','Ticker','Symbol','Market Cap',f"Market Cap ({target_currency})",'Beta','Average Volume','Shares Outstanding','P/E Ratio','Yield(%)','Public Float']].copy()
    return Financial_df1, industry_and_currency_df