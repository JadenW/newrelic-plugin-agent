"""
VoltDB

"""
import logging
import requests
import yaml
import math
import sys
from struct import unpack_from

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class HdrHistogram():
    """ A python implemenation of High Dynamic Range (HDR) Histogram.
    Based on http://hdrhistogram.github.io/HdrHistogram/. """

    def __init__(self, 
                lowest_trackable_value,
                highest_trackable_value,
                number_significant_value_digits,
                total_count):
        self.lowest_trackable_value = lowest_trackable_value
        self.highest_trackable_value = highest_trackable_value
        self.number_significant_value_digits = number_significant_value_digits
        self.total_count = total_count

        largest_value_with_single_unit_resolution = 2 * math.pow(10, number_significant_value_digits)
        self.unit_magnitude = long(math.floor(math.log(lowest_trackable_value,2))) 
        sub_bucket_count_magnitude = long(math.ceil(math.log(largest_value_with_single_unit_resolution,2)))
        self.sub_bucket_half_count_magnitude = sub_bucket_count_magnitude - 1 if sub_bucket_count_magnitude > 1 else 0
        self.sub_bucket_count = int(math.pow(2, self.sub_bucket_half_count_magnitude + 1))
        self.sub_bucket_half_count = self.sub_bucket_count / 2
        self.bucket_count = self.get_bucket_count(highest_trackable_value,
                                             self.sub_bucket_count,
                                             self.unit_magnitude)
        self.counts_len = (self.bucket_count + 1) * (self.sub_bucket_half_count)
        self.count = [0] * self.counts_len

    def get_bucket_count(self,value, subb_count, unit_mag):
        smallest_untrackable_value = (subb_count-1) << unit_mag
        buckets_needed = 1
        while smallest_untrackable_value < value:
            if smallest_untrackable_value > sys.maxint / 2:
                return buckets_needed + 1
            smallest_untrackable_value <<= 1
            buckets_needed += 1
        return buckets_needed

    def diff(self,newer):
        h = HdrHistogram(newer.lowest_trackable_value, 
                         newer.highest_trackable_value,
                         newer.number_significant_value_digits,
                         newer.total_count - self.total_count)
        for i in xrange(h.counts_len):
            h.count[i] = newer.count[i] - self.count[i]
        return h
   
    def get_count_at_index(self, bucket_index, sub_bucket_index):
        bucket_base_index = (bucket_index + 1) << self.sub_bucket_half_count_magnitude
        offset_in_bucket = sub_bucket_index - self.sub_bucket_half_count
        counts_index = bucket_base_index + offset_in_bucket
        return self.count[counts_index]
   
    def get_value_from_index(self, bucket_index, sub_bucket_index):
        return sub_bucket_index << (bucket_index + self.unit_magnitude)

    def get_value_at_percentile(self, percentile):
        '''Get the value for a given percentile
        Args:
            percentile: a float in [0.0..100.0]
        Returns:
            the value for the given percentile
        '''
        requested_percentile = percentile if percentile < 100.0 else 100.0
        count_at_percentile = int(((requested_percentile / 100.0) * self.total_count) + 0.5)
        count_at_percentile = max(count_at_percentile, 1)
        total_to_current = 0
        for i in xrange(self.bucket_count):
            j0 = 0 if i == 0 else self.sub_bucket_half_count
            for j in xrange(j0,self.sub_bucket_count):
                total_to_current += self.get_count_at_index(i,j)
                if total_to_current >= count_at_percentile:
                    value_at_index = self.get_value_from_index(i,j)
                    return value_at_index / 1000.0

        return 0.0
    
    @classmethod
    def convert_bytes_to_histogram(cls, byte_buffer, limit):
        offset = 0
        lowest_trackable_value = unpack_from("<Q", byte_buffer, offset)[0]
        offset += 8
        highest_trackable_value = unpack_from("<Q", byte_buffer, offset)[0]
        offset += 8 
        number_significant_value_digits = unpack_from("<I", byte_buffer, offset)[0] 
        offset += 4
        total_count = unpack_from("<Q", byte_buffer, offset)[0]
        offset += 8 
        LOGGER.debug("create the new hist with %d,%d,%d,%d", lowest_trackable_value,
                    highest_trackable_value, 
                    number_significant_value_digits,
                    total_count)

        hist = cls(lowest_trackable_value, 
                highest_trackable_value, 
                number_significant_value_digits, 
                total_count)

        i = 0
        while (limit - offset >= 8):
            hist.count[i] = unpack_from("<Q", byte_buffer, offset)[0]
            LOGGER.debug("add val %d to bucket %d:", hist.count[i],i)
            offset += 8
            i += 1

        return hist

class VoltDB(base.JSONStatsPlugin):

    DEFAULT_HOST = 'localhost'
    DEFAULT_PATH = '/api/1.0/'
    DEFAULT_QUERY = 'Procedure=@Statistics&Parameters=["MANAGEMENT",0]'
    DEFAULT_PORT = 8080
    GUID = 'com.voltdb.database.VoltDB.json'

    def add_datapoints(self, stats):
        """Add all of the datapoints for the VoltDB poll

        :param dict stats: The stats to process for the values

        """
        LOGGER.debug('Stats: %r', stats)
        self.add_latency_stats()
        self.add_connection_stats()
        self.add_mem_stats("Memory", ["MEMORY",0])
        self.add_stats("PP", ["PROCEDUREPROFILE",0])
        self.add_stats("Table", ["TABLE",0])


    def add_stats(self,namespace, parameters,
            ignore_columns = ["timestamp", "site_id", "partition_id", "weighted_perc", "host_id"]
            ):
        """This method handles default processing of statistics aggregrating them on column and reporting to newrelic. 
        Namespace define what the metric reported as e.g. "Components/VoltDB/" + namespace + "metric_name"""
        # aggregate the count
        url = self.stats_url.replace(self.DEFAULT_QUERY,"Procedure=@Statistics&Parameters=" + str(parameters))
        response = self.http_get(url)
        proc_value = dict()
        if response.status_code == 200:
            data = response.json()
            table = data['results'][0]
            table_schema = table['schema']
            table_data = table['data']
            for r in xrange(len(table_data)):
                if table_data[r] is None:
                    continue
                ccount = len(table_schema)
                for i in range(ccount): 
                    vtype = table_schema[i]['type']
                    cname = table_schema[i]['name']
                    if (cname.lower() in ignore_columns):
                        continue
                    key = "VoltDB/" + namespace + "/" + cname
                    old_val = proc_value.get(key)   
                    # ? type already parsed via json decoder
                    # cvalue = yaml.load(table_data[i]) 
                    cvalue = table_data[r][i]
                    if old_val is not None and type(old_val) != type(cvalue):
                        LOGGER.warn("data type changed for column %d, before was %s, now is %s", i,type(old_val), type(cvalue))
                    if type(cvalue) not in [int, long, float]:
                        LOGGER.warn("unrecongized data type changed for column %d, is %s", i, type(cvalue))
                        cname = None
                    if cname is not None:
                        if old_val is not None and cvalue is not None:
                            cvalue += old_val
                        proc_value[key] = cvalue

                        
            for key in proc_value.keys():
                LOGGER.debug("Push metric %s with value %d", key, proc_value.get(key))
                self.add_gauge_value(key, 'value', proc_value.get(key))
        else:
            LOGGER.error('Error collecting %sstats (%s): %s', namespace,
                         response.status_code, response.content)

    def add_mem_stats(self,namespace, parameters,
            ignore_columns = ["timestamp", "site_id", "partition_id", "tuplecount", "host_id"]
            ):
        """ Memory reporting does not use base method as we dont report TUPLECOUNT and units are not consistent e.g.
        POOLEDMEMORY is in MB and rest in KB."""
        url = self.stats_url.replace(self.DEFAULT_QUERY,"Procedure=@Statistics&Parameters=" + str(parameters))
        response = self.http_get(url)
        proc_value = dict()
        if response.status_code == 200:
            data = response.json()
            table = data['results'][0]
            table_schema = table['schema']
            table_data = table['data']
            for r in xrange(len(table_data)):
                if table_data[r] is None:
                    continue
                ccount = len(table_schema)
                for i in range(ccount): 
                    vtype = table_schema[i]['type']
                    cname = table_schema[i]['name']
                    if (cname.lower() in ignore_columns):
                        continue
                    key = "VoltDB/" + namespace + "/" + cname
                    old_val = proc_value.get(key)   
                    # ? type already parsed via json decoder
                    # cvalue = yaml.load(table_data[i]) 
                    multiplier = 1024*1024 if cname.upper() == "POOLEDMEMORY" else 1024
                    cvalue = table_data[r][i] 
                    if old_val is not None and type(old_val) != type(cvalue):
                        LOGGER.warn("data type changed for column %d, before was %s, now is %s", i,type(old_val), type(cvalue))
                    if type(cvalue) not in [int, long, float]:
                        LOGGER.warn("unrecongized data type changed for column %d, is %s", i, type(cvalue))
                        cname = None
                    else:
                        cvalue *= multiplier
                    if cname is not None:
                        if old_val is not None and cvalue is not None:
                            cvalue += old_val
                        proc_value[key] = cvalue

                        
            for key in proc_value.keys():
                LOGGER.debug("Push metric %s with value %d", key, proc_value.get(key))
                self.add_gauge_value(key, 'bytes', proc_value.get(key))
        else:
            LOGGER.error('Error collecting %sstats (%s): %s', namespace,
                         response.status_code, response.content)

    def add_latency_stats(self):
        namespace = "Latency"
        parameters = ['LATENCY_HISTOGRAM',0]
        url = self.stats_url.replace(self.DEFAULT_QUERY,"Procedure=@Statistics&Parameters=" + str(parameters))
        response = self.http_get(url)
        if response.status_code == 200:
            data = response.json()
            uncompressed_histogram = data['results'][0]['data'][0][5].decode("hex")
            byte_buffer = buffer(uncompressed_histogram)
            hist = HdrHistogram.convert_bytes_to_histogram(byte_buffer, 
                    len(uncompressed_histogram))            
            LOGGER.debug("Push metric %s with value %d", 'VoltDB/'+namespace, 
                    hist.get_value_at_percentile(99))
            self.add_derive_histogram_value('VoltDB/' + namespace, 'milliseconds', hist)
        else:
            LOGGER.error('Error collecting %sstats (%s): %s', namespace,
                         response.status_code, response.content)

    def add_connection_stats(self):
        """Add stats that go under Component/Connections"""
        namespace = "Connections"
        parameters = ['LIVECLIENTS',0]
        url = self.stats_url.replace(self.DEFAULT_QUERY,"Procedure=@Statistics&Parameters=" + str(parameters))
        response = self.http_get(url)
        if response.status_code == 200:
            data = response.json()
            l = len(data['results'][0]['data'])
            self.add_gauge_value('VoltDB/' + namespace, 'value',
                                l-1) # Take out our own connection. 
        else:
            LOGGER.error('Error collecting connection stats (%s): %s',
                         response.status_code, response.content)

    def add_derive_histogram_value(self, metric_name, units, value, count=None):
        """Add a value that will derive the current value from the difference
        between the last interval value and the current value.

        If this is the first time a stat is being added, it will report a 99% latency 
        value until the next poll interval and it is able to calculate the
        derivative value.

        :param str metric_name: The name of the metric
        :param str units: The unit type
        :param int value: The value to add
        :param int count: The number of items the timing is for

        """
        if value is None:
            value = HdrHistogram()
        metric = self.metric_name(metric_name, units)
        if metric not in self.derive_last_interval.keys():
            LOGGER.debug('Bypassing initial %s value for first run', metric)
            self.derive_values[metric] = self.metric_payload(value.get_value_at_percentile(99), count=0)
        else:
            cval = self.derive_last_interval[metric].diff(value).get_value_at_percentile(99)
            self.derive_values[metric] = self.metric_payload(cval, count=count)
            LOGGER.debug('%s: Last: %r, Current: %r, Reporting: %r',
                         metric, self.derive_last_interval[metric].get_value_at_percentile(99), 
                         value.get_value_at_percentile(99),
                         self.derive_values[metric])
        self.derive_last_interval[metric] = value
    
