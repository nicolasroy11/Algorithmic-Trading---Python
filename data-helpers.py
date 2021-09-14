from numpy import float64
import pandas as pd
from typing import List, Tuple, Any
from scipy import stats

p_value = float64
correlation = float64

class Intervals:
    one_minute = '1m'
    five_minutes = '5m'

class Exchange:
    name: str
    baseApiUrl: str
    symbolHistoryDataColumnNames: List[str]
    symbolHistoryDateColumns: List[str]
    
    def __init__(self, name, baseApiUrl, apiDataColumnNames, symbolHistoryDateColumns):
        self.name = name
        self.baseApiUrl = baseApiUrl
        self.symbolHistoryDataColumnNames = apiDataColumnNames
        self.symbolHistoryDateColumns = symbolHistoryDateColumns
    
    def get_symbol_history_url(self, symbol: str, interval: Intervals):
        return self.baseApiUrl + f'klines?symbol={symbol}&interval={interval}'

class Exchanges:
    binance = Exchange(
        'binance',
        'https://api.binance.com/api/v3/',
        ['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Closetime', 'Quote asset volume', 'Number of trades','Taker by base', 'Taker buy quote', 'Ignore'],
        ['Opentime', 'Closetime'])

def read_csv_from_path(path: str, parseDates: List[str] or bool = False) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates = parseDates)

def get_dataframe_meta(df: pd.DataFrame) -> Any:
    return df.info()

def get_df_cell(df: pd.DataFrame, x: int, y: int):
    return (df.iloc[x, y], type(df.iloc[x, y]))

def get_symbol_history(exchange: Exchange, symbol: str, interval: Intervals) -> pd.DataFrame:
    df = pd.DataFrame()
    url = exchange.get_symbol_history_url(symbol, interval)
    
    df2 = pd.DataFrame = pd.read_json(url)
    df2.columns = exchange.symbolHistoryDataColumnNames
    df = pd.concat([df2, df], axis=0, ignore_index=True, keys=None)   
    df.reset_index(drop=True, inplace=True)
    return df2

def correlate_time_series(a: pd.Series, b: pd.Series) -> Tuple[correlation, p_value]:
    return stats.pearsonr(a, b)

def autocorrelate_time_series(series: pd.Series, numOfPeriods: int) -> Tuple[correlation, p_value]:
    return stats.pearsonr(series, series.shift(periods = numOfPeriods))

def normalize_df(series: pd.Series) -> pd.DataFrame:
    return series.div(series.iloc[0]).mul(100) # vectorized operation that divides every value in a table by those of the first row
                                                # for each respective column; useful for comparing stocks with wildly differing 
                                                # price ranges

def get_percent_change(df: pd.Series, numOfPeriods: int) -> pd.Series:
    return df.pct_change(periods = numOfPeriods).mul(100)    # returns a series of the %change after numOfPeriods

btc_usdt: pd.DataFrame = get_symbol_history(Exchanges.binance, 'BTCUSDT', Intervals.five_minutes)
# print(correlate_time_series(btc_usdt['Close'], btc_usdt['Close'].shift(periods = 1)))

# btc_usdt = btc_usdt.set_index(pd.to_datetime(btc_usdt['Opentime']))

print(get_percent_change(btc_usdt['Close'], 1))

