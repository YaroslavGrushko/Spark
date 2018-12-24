from __future__ import print_function
import json
import sys
from operator import add

import random
import csv
#standart module
import time
from datetime import datetime, timedelta
from random import randint

import numpy as np
import pandas
import geopy.distance

# from random_word import RandomWords
import random


DRIVER_COMMENT_BY_RATE = {
    5: ('p', 'l', 'n', 's', 't', 'g'),
    4: ('p', 'l','n'),
    3: ('p', 'l', 's', 't'),
    2: ('r', 'i', 's', 't'),
    1: ('r', 'i', 's', 't'),
}

CLIENT_COMMENT_BY_RATE = {
    5: ('p', 'g', 'n', 's', 't'),
    4: ('p', 'g','n'),
    3: ('p', 'g', 's', 't'),
    2: ('r', 'b', 's', 't'),
    1: ('r', 'b', 's', 't'),
}
TEXT_COMMENTS = ["dummy text 1", "dummy text 2", "dummy text 3"]

BATCH_NUMBER = 800

# distances
# london_addresses = pandas.read_csv('London postcodes.csv')
filename = "London postcodes.csv"
n = sum(1 for line in open(filename)) - 1 #number of records in file (excludes header)
s = 20 #desired sample size
skip = sorted(random.sample(range(1,n+1),n-s)) #the 0-indexed header will not be included in the skip list
london_addresses = pandas.read_csv(filename, skiprows=skip)

DT_FORMAT = '%m/%d/%Y %I:%M %p'


# trip duration
def strTimeProp(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(format, time.localtime(ptime))


def randomDate(start, end, prop):
    return strTimeProp(start, end, DT_FORMAT, prop)


# money
cost = lambda x: 10 + (2 if (x<= 6) else 4 if (x <=10) else 3 if (x<=16) else 4 if (x<=20) else 3)


def calculate_cost(start_datetime):
    date = datetime.strptime(start_datetime, '%m/%d/%Y %I:%M %p')
    cost_rate = cost(date.hour + date.minute / 60)
    return round(2.40 +  cost_rate, 2)


# r = RandomWords()

def generate_trip(curr_index):
    
    rand1 =randint(0, len(london_addresses) - 1)
    rand2 = randint(0, len(london_addresses) - 1)
    startPoint = {
        'latitude': london_addresses['Latitude'][rand1],
        'longitude': london_addresses['Longitude'][rand1]
    }
    endPoint = {
        'latitude': london_addresses['Latitude'][rand2],
        'longitude': london_addresses['Longitude'][rand2]
    }

    first_data = {
        'driver': randint(0, 3000),
        'client': randint(0, 5000),
        'start_point': startPoint,
        'end_point': endPoint,
        'start_datetime': randomDate("1/9/2018 1:30 PM", "1/12/2018 4:50 AM", random.random()),
        'driver_rate': np.random.choice([None, 1, 2, 3, 4, 5], p=[2 / 16, 1 / 16, 1 / 16, 2 / 16, 3 / 16, 7 / 16]),
        'client_rate': np.random.choice([None, 1, 2, 3, 4, 5], p=[2 / 16, 1 / 16, 1 / 16, 2 / 16, 3 / 16, 7 / 16])
    }
    trip = fulfil_trip(first_data, curr_index)
    return trip


def fulfil_trip(trip, curr_index):
    s_lat = trip['start_point']['latitude']
    s_long = trip['start_point']['longitude']
    e_lat = trip['end_point']['latitude']
    e_long = trip['end_point']['longitude']

    trip['end_datetime'] = datetime.strftime(datetime.strptime(trip['start_datetime'], '%m/%d/%Y %I:%M %p') \
                                             + timedelta(minutes=randint(20, 180)),
                                             '%m/%d/%Y %I:%M %p')
    trip['cost'] = calculate_cost(trip['start_datetime'])
    try:
        temp = DRIVER_COMMENT_BY_RATE[trip['driver_rate']]
        trip['driver_comment'] = temp[randint(0, len(temp) - 1)]
    except:
        pass
    try:
        temp = CLIENT_COMMENT_BY_RATE[trip['client_rate']]
        trip['client_comment'] = temp[randint(0, len(temp) - 1)]
    except:
        pass
    myIndex = curr_index % 3
    trip['driver_text_comment'] = TEXT_COMMENTS[myIndex]  #r.get_random_word()
    return trip

def toCSVLine(data):
    return ','.join(str(d) for d in data)


if __name__ == "__main__":
   file = open("testfile.csv", "a")
   for i in range(3000):
        a = generate_trip(i)
        file.write(json.dumps(a) + '\n')

   file.close()