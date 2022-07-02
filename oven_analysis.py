import pandas as pd
from IP21Connect_v2 import IP21Connector
import getpass
from Capacity import CureCapacity

from Params import *

def login_IP21():
  user = input("Username: ")
  domain_user = 'NA\\' + user
  #print(domain_user)
  pwd = getpass.getpass()
  db = input("Database name: ")
  
  aspen = IP21Connector(server = db, user=domain_user, pw=pwd)
  
  return aspen

def main():
    aspen = login_IP21()
    chutes = 'WE1_NO_CHUTES'

    zone_a = CureCapacity(
        name = 'Zone A',
        max_temp = 600,
        max_fan = 1167,
        tag_temp = 'WE1_PI_OV_ZONE_A_TEMP',
        tag_fan = 'WE1_PI_OV_FAN_ZA_SPD',
        tag_chutes = chutes
    )

    zone_b = CureCapacity(
        name = 'Zone B',
        max_temp = 600,
        max_fan = 1167,
        tag_temp = 'WE1_PI_OV_ZONE_B_TEMP',
        tag_fan = 'WE1_PI_OV_FAN_ZA_SPD',
        tag_chutes = chutes
    )

    aspen.connect()
    df_a = zone_a.tag_history(aspen, timestamp, period)
    df_a = df_a.loc[df_a['WE1_NO_CHUTES'] <= 2]
    df_a = df_a.reset_index(drop=True)

    df_b = zone_b.tag_history(aspen, timestamp, period)
    df_b = df_b.loc[df_b['WE1_NO_CHUTES'] <= 2]
    df_b = df_b.reset_index(drop=True)

    a_capacity, a_percent = zone_a.zone_capacity(series_temp = df_a[zone_a.tag_temp], series_fan = df_a[zone_a.tag_fan])
    b_capacity, b_percent = zone_b.zone_capacity(series_temp = df_b[zone_b.tag_temp], series_fan = df_b[zone_b.tag_fan])

    print('-------------------------------------------------')
    print('-------------------------------------------------')
    print('Zone A % Capacity: ' + str(a_percent))
    print('Zone B % Capacity: ' + str(b_percent))
    oven_percent = (float(a_percent)+float(b_percent))/2
    oven_percent = "{:.2f}".format(oven_percent)
    print('Oven % Capacity: ' + str(oven_percent))

    print('-------------------------------------------------')
    print('-------------------------------------------------')

    a_group = zone_a.group_capacity(
        series_timestamp = df_a['timestamp'],
        series_temp = df_a[zone_a.tag_temp], 
        series_fan = df_a[zone_a.tag_fan]
    )
    b_group = zone_b.group_capacity(
        series_timestamp = df_b['timestamp'],
        series_temp = df_b[zone_b.tag_temp], 
        series_fan = df_b[zone_b.tag_fan]
    )

    print(a_group)
    print(b_group)

if __name__ == '__main__':
    main()