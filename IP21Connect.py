from requests import Session				#officially supports python 2.7 - pypi
from requests.auth import HTTPBasicAuth
from zeep import Client 				#zeep 3.4.0 for python 2.7 - pypi
from zeep.transports import Transport
import xml.etree.ElementTree as ET			#version 2.5 - standard library
from dateutil import relativedelta			#version 2.8.2 supports python 2.7 - pypi			
from datetime import datetime, timedelta, timezone	#version 2.3 - standard library
from math import ceil					#always available - standard library


class IP21Connector(object):
    def __init__(self, server, user, pw):
        self.user = user
        self.pw = pw
        self.server = server
        self.client = ''

    def connect(self):
        session = Session()
        session.auth = HTTPBasicAuth(self.user, self.pw)
        url = 'http://' + self.server + '/SQLPlusWebService/SQLPlusWebService.asmx'
        self.client = Client(url + '?WSDL', transport=Transport(session=session))
    
    @staticmethod
    def parse_xml(root):
        all_records = []
        for i, child in enumerate(root):
            record = {}
            for sub_child in child:
                record[sub_child.tag] = sub_child.text
            all_records.append(record)
        return all_records

    def agg_query(self, tag_name, ts):
        parsed_data = []

        sql = """
		SELECT
			AVG "value"
		FROM 
			Aggregates
		WHERE 
			name = '{tag_name}'
			and Period = 00:05
			and TS_Start > '{ts}'
			order by TS_Start desc""".format(tag_name=tag_name, ts=ts)

        result = self.client.service.ExecuteSQL(sql)
        parsed_data = self.parse_xml(ET.fromstring(result))
        
        return parsed_data

    def get_history(self, timestamp, tag_name, period):
        parsed_data = []

        sql = """
		Select
			ts,
			value
		From
			History
		Where
			name = '{tag_name}'
			and ts > '{timestamp}'
			and period = {period}""".format(timestamp=timestamp, tag_name=tag_name, period=period)

        result = self.client.service.ExecuteSQL(sql)
        parsed_data = self.parse_xml(ET.fromstring(result))
        
        return parsed_data
