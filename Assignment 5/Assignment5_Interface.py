#
# Assignment5 Interface
# Name: Sethu Manickam 
#

from pymongo import MongoClient
import os
import sys
import json
from math import radians, sin, cos, atan2, sqrt

def FindDist(lat_inp, lon_inp, lat_bus, lon_bus):
    delta_phi = radians(lat_bus - lat_inp)
    delta_lambda = radians(lon_bus - lon_inp)
    phi1 = radians(lat_inp)
    phi2 = radians(lat_bus)
    a = (sin(delta_phi/2) * sin(delta_phi/2)) + (cos(phi1) * cos(phi2) * sin(delta_lambda/2) * sin(delta_lambda/2))
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return 3959 * c

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    find_business_city = collection.find({'city': {'$regex':cityToSearch, '$options':"$i"}})
    with open(saveLocation1, "w") as file:
        for bus_tuple in find_business_city:
            writeString = bus_tuple['name'].upper() + "$" + bus_tuple['full_address'].replace("\n", ", ").upper() + "$" + bus_tuple['city'].upper() + "$" + bus_tuple['state'].upper() + "\n"
            file.write(writeString)

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    lat_inp = float(myLocation[0])
    lon_inp = float(myLocation[1])
    find_business_loc = collection.find({'categories':{'$in': categoriesToSearch}}, {'name': 1, 'latitude': 1, 'longitude': 1, 'categories': 1})
    with open(saveLocation2, "w") as file:
        for bus_tuple in find_business_loc:
            name = bus_tuple['name']
            d = FindDist(lat_inp, lon_inp, float(bus_tuple['latitude']), float(bus_tuple['longitude']))
            if d <= maxDistance:
                writeRow = name.upper() + "\n"
                file.write(writeRow)