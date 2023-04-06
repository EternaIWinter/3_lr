import requests

cnb_daily_url = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/daily.txt"

def parse(text):
    currencies = {}
    strings = text.strip().split('\n')[2:]
    for str in strings:
        elem_str = str.split('|')
        if len(elem_str) != 5:
            continue
        counrty, currency, amount, code, rate = elem_str
        currency = code
        rate = float(rate) / float(amount)
        currencies[currency] = rate
    return currencies

def get_values(day):
    response = requests.get(cnb_daily_url, params={"date": day.strftime("%d.%m.%Y")}) 
    return parse(response.text)

