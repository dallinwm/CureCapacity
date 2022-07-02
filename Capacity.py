import pandas as pd
from IP21Connect_v2 import IP21Connector
import statistics

class CureCapacity(object):
    def __init__(self, name, max_temp, max_fan, tag_fan, tag_temp, tag_chutes):
        self.name = name
        self.max_temp = max_temp
        self.max_fan = max_fan
        self.tag_fan = tag_fan
        self.tag_temp = tag_temp
        self.tag_chutes = tag_chutes
        
    @staticmethod
    def calc_capacity(series_temp, series_fan, max_fan, max_temp):
        capacity = []
        for i in range(len(series_temp)):
            capacity.append((series_fan.iloc[i] / max_fan + series_temp.iloc[i] / max_temp) / 2)
        return capacity
        
    def tag_history(self, aspen, timestamp, period):
	#aspen.connect()
        fan_value = aspen.get_history(timestamp, self.tag_fan, period)
        temp_value = aspen.get_history(timestamp, self.tag_temp, period)
        chutes_in = aspen.get_history(timestamp, self.tag_chutes, period)
        
        fan_df = pd.DataFrame(fan_value).astype({'value': 'float'})
        fan_df = fan_df.rename(columns={'ts': 'timestamp', 'value': self.tag_fan})
        temp_df = pd.DataFrame(temp_value).astype({'value': 'float'})
        temp_df = temp_df.rename(columns={'ts': 'timestamp', 'value': self.tag_temp})
        chutes_df = pd.DataFrame(chutes_in).astype({'value': 'float'})
        chutes_df = chutes_df.rename(columns={'ts': 'timestamp', 'value': self.tag_chutes})

        df = fan_df.merge(temp_df, on='timestamp', how='left')
        df = df.merge(chutes_df, on='timestamp', how='left')
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
        
    def zone_capacity(self, series_temp, series_fan):
        capacity = self.calc_capacity(series_temp, series_fan, self.max_fan, self.max_temp)
        
        percent = statistics.mean(capacity) * 100
        percent = "{:.2f}".format(percent)
        
        return capacity, percent
    
    def group_capacity(self, series_timestamp, series_temp, series_fan):
        capacity = self.calc_capacity(series_temp, series_fan, self.max_fan, self.max_temp)
        df = pd.DataFrame(capacity)
        ts_df = pd.DataFrame(series_timestamp)
        group_df = pd.concat([ts_df, df], axis = 1, ignore_index = True)
        group_df = group_df.rename(columns={0: 'timestamp', 1: self.name + " Capacity"})
        
        group_df = group_df.groupby(pd.Grouper(key="timestamp", freq="1W")).mean()
        
        return group_df