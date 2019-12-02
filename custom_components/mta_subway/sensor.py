"""
Sensor for checking the status of NYC MTA Subway lines.
"""

import logging
import re
from datetime import timedelta

import homeassistant.helpers.config_validation as cv
import requests
import voluptuous as vol
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.helpers.entity import Entity
from homeassistant.util import Throttle

import csv
from google.transit import gtfs_realtime_pb2
import json
from collections import defaultdict
import math
from requests_toolbelt.threaded import pool
import time
from typing import List

_LOGGER = logging.getLogger(__name__)

CONF_API_KEY = "api_key"
CONF_STATION = "station"
SCAN_INTERVAL = timedelta(seconds=20)

# http://datamine.mta.info/list-of-feeds
FEED_IDS = [1, 26, 16, 21, 2, 11, 31, 36, 51 ]


PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Required(CONF_STATION): cv.ensure_list,
    vol.Required(CONF_API_KEY): cv.string
})


def setup_platform(hass, config, add_devices, discovery_info=None):
    """ Sets up the MTA Subway sensors.
    """
    api_key = config.get(CONF_API_KEY)
    stations = config.get(CONF_STATION)
    data = MTASubwayData(api_key, stations)
    data.update()
    sensors = [
        MTASubwaySensor(station, data)
        for station in config.get(CONF_STATION)
    ]
    add_devices(sensors, True)


class MTASubwaySensor(Entity):
    """ Sensor that reads the status for an MTA Subway line.
    """
    def __init__(self, station_id, data):
        """ Initalize the sensor.
        """
        self._name = None
        self._station_id = station_id
        self._data = data
        self._state = None
        self._arrivals = None
        self._station_name = None
        self._station_direction_label = None

    @property
    def name(self):
        """ Returns the name of the sensor.
        """
        return self._name

    @property
    def state(self):
        """ Returns the state of the sensor.
        """
        return self._state

    @property
    def icon(self):
        """ Returns the icon used for the frontend.
        """
        return "mdi:subway"

    @property
    def entity_picture(self):
        """ Returns the icon used for the frontend.
        """
        try: 
            line = self._arrivals[0]["line"].lower()[0]
            return "/community_plugin/lovelace-mta-subway-realtime/public/{}.svg".format(line)
        except Exception:
            _LOGGER.debug("Couldn't set entity_picture")
            return None

    @property
    def device_class(self):
        return "timestamp"

    @property
    def device_state_attributes(self):
        """ Returns the attributes of the sensor.
        """
        attrs = {}
        attrs["arrivals"] = self._arrivals
        attrs["station_name"] = self._station_name
        attrs["station_direction_label"] = self._station_direction_label
        return attrs
    
    @property
    def unit_of_measurement(self):
        try: 
            return "{} min".format(self._data.data["arrivals"][self._station_id][0]["time_until"])
        except Exception:
            _LOGGER.debug("Failed to set unit_of_measurement")
            return None

    @property
    def unique_id(self):
        return "mta_subway_" + self._station_id

    def update(self):
        """ Updates the sensor.
        """
        self._data.update()
        station_data = self._data.data["arrivals"][self._station_id]
        try:
            self._state = station_data[0]["time"]
            self._arrivals = self._data.data["arrivals"][self._station_id]

            stop_id_without_direction = self._station_id[:-1]
            
            if self._station_id.endswith("N"):
                self._station_direction_label = self._data.data["stations"][stop_id_without_direction]["north_direction_label"]
                self._name = "MTA: " + self._data.data["stations"][stop_id_without_direction]["stop_name"] + " Uptown"
            elif self._station_id.endswith("S"):
                self._station_direction_label = self._data.data["stations"][stop_id_without_direction]["south_direction_label"]
                self._name = "MTA: " + self._data.data["stations"][stop_id_without_direction]["stop_name"] + " Downtown"
            else:
                raise Exception("Bad station id: {}".format(self._station_id))

            self._station_name = self._data.data["stations"][stop_id_without_direction]["stop_name"]

            #self._name = "MTA Subway Station " + self._station_name + " towards " + self._station_direction_label
        except Exception:
            _LOGGER.exception("Error updating sensor")


class MTASubwayData(object):
    """ Query MTA Subway data from the XML feed.
    """


    def __get_stations(self):
        _LOGGER.debug("Fetching station information")
        url = "http://web.mta.info/developers/data/nyct/subway/Stations.csv"
        r = requests.get(url)
        reader = csv.DictReader(r.text.splitlines(), delimiter=',')
        result = {}
        for row in reader:
            stop_name = row["Stop Name"]
            result[row["GTFS Stop ID"]] = {
                "stop_name": stop_name,
                "south_direction_label": row["South Direction Label"],
                "north_direction_label": row["North Direction Label"],
                "complex_id": row["Complex ID"]
            }
            #result[row["GTFS Stop ID"]+"N"] = stop_name + " - " + row["North Direction Label"]
        _LOGGER.debug("Got station information")
        return result

    def __get_realtime_data(self) -> List[gtfs_realtime_pb2.FeedMessage]:
        _LOGGER.debug("Fetching realtime data...")
        urls = ['http://datamine.mta.info/mta_esi.php?key={}&feed_id={}'.format(self.api_key, feed_id) for feed_id in FEED_IDS]

        for url in urls:
            _LOGGER.debug("GET {}".format(url))
        p = pool.Pool.from_urls(urls)
        p.join_all()

        data = []

        for response in p.responses():
            #_LOGGER.debug('Fetched realtime data: GET {0}. Returned {1}.'.format(response.request_kwargs['url'], response.status_code))
            if response.status_code != 200:
                _LOGGER.error("Non-200 response while fetching {}".format(response.request_kwargs['url']))
            else:
                try:
                    feed = gtfs_realtime_pb2.FeedMessage()
                    feed.ParseFromString(response.content)
                    data.append(feed)

                except Exception:
                    _LOGGER.exception("Failure to parse fetched message")
                    _LOGGER.debug(response.text)

        return data

    def __init__(self, api_key, watched_stations):

        self.data = None
        self.api_key = api_key
        self.watched_stations = watched_stations

    @Throttle(SCAN_INTERVAL)
    def update(self):
        """ Update data based on SCAN_INTERVAL.
        """

        stations = self.__get_stations()
        realtime_data = self.__get_realtime_data()


        arrivals = defaultdict(list)

        current_time = int(time.time())

        for feed in realtime_data:
            header = feed.header

            for entity in feed.entity:
                if entity.HasField("trip_update"):
                    trip = entity.trip_update.trip
                    
                    for stop_time_update in entity.trip_update.stop_time_update:
                        stop_id = stop_time_update.stop_id
                        stop_id_without_direction = stop_id[:-1]

                        if stop_id in self.watched_stations and stop_id_without_direction in stations:
                            arrivals[stop_id].append({
                                "time": stop_time_update.arrival.time,
                                "line": trip.route_id,
                                "last_updated": header.timestamp,
                                "time_until": math.ceil(float(stop_time_update.arrival.time - current_time) / 60)
                                })

        for k,v in arrivals.items():
            v.sort(key=lambda x:x["time"])
        
        self.data = {
            "stations": stations,
            "arrivals": arrivals
        }


