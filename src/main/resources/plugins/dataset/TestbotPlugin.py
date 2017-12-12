"""
Copyright (c) 2017 Cisco and/or its affiliates.
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of
Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright,
international treaties, patent, and/or contract. Any use of the material herein must be in
accordance with the terms of the License.
All rights not expressly granted by the License are reserved.
Unless required by applicable law or agreed to separately in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.
Purpose:    Gobblin/Dataset tests
"""

import time
import argparse
import sys
import requests
import json
import os
import re
import socket
import os.path
import logging
import datetime

from pnda_plugin import PndaPlugin
from pnda_plugin import Event

sys.path.insert(0, '../..')
TestbotPlugin = lambda: DatasetWhitebox()
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
HERE = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("TestbotPlugin")


class DatasetWhitebox(PndaPlugin):
    '''
    Whitebox test plugin for Master Dataset Health
    '''

    def __init__(self):
        self.display = False
        self.results = []
        self.runtesttopic = "avro.internal.testbot"
        self.cluster_name = None
        self.data_service_ip = None
        self.cron_interval = None
        self.gobblin_log_path = None
        self.hdfs_url = None
        self.elk_ip = None
        self.metric_console = None
        self.num_attempts = None
        self.uri = None
        self.query = None
        self.file_path = '/var/log/master_dataset.txt'
        self.now_year = None
        self.now_month = None
        self.now_day = None
        self.now_hour = None
        self.before_year = None
        self.before_month = None
        self.before_day = None
        self.before_hour = None

    def read_args(self, args):
        '''
            This class argument parser.
            This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(
            prog=self.__class__.__name__,
            usage='%(prog)s [options]',
            description='Show state of Dataset cluster',
            add_help=False)
        parser.add_argument('--cron_interval', default=30,
                            help='CRON interval that has been set for Gobblin run: 30')
        parser.add_argument('--data_service', default='http://10.0.1.20:7000', help='DATA_SERVICE IP ADDRESS')
        parser.add_argument('--cluster_name', default='cluster_name', help="PNDA CLUSTER NAME")
        parser.add_argument('--gobblin_log_path', default='/var/log/pnda/gobblin/gobblin-current.log',
                            help="Gobblin Log path")
        parser.add_argument('--elk_ip', default='localhost:9200', help="Elasticsearch IP address")
        parser.add_argument('--metric_console', default='localhost:3123', help="Console metric IP address")
        parser.add_argument('--num_attempts', default=10, help="retry count to check gobblin health")
        return parser.parse_args(args)

    # Get Kafka health metric from KAFKA platform-testing
    def get_kafka_health(self):
        kafka_health = False
        url = self.metric_console + '/metrics/kafka.health'
        try:
            response = requests.get(url)
            if response.status_code is 200:
                kafka_health_dict = json.loads(response.text)
                if kafka_health_dict['currentData']['value'] == 'OK':
                    kafka_health = True
        except Exception as e:
            LOGGER.error("Exception while checking KAFKA health due to %s", e)
        return kafka_health

    # Query elasticsearch to get log file which is specific to gobblin parameters
    def query_es(self):
        hits = []
        try:
            response = requests.post(self.uri, data=self.query)
            if response.status_code is 200:
                results = json.loads(response.text)
                hits = results.get('hits', {}).get('hits', [])
        except Exception as e:
            LOGGER.error("Exception while querying elasticsearch due to %s", e)
        return hits

    # Write status variables to a file to track each operation
    def save_status_vars(self, status_dict):
        try:
            with open(self.file_path, 'w') as f:
                f.write(json.dumps(status_dict))
        except Exception as e:
            LOGGER.error("Exception while saving status variables to file due to %s", e)

    # Reading contents from a status file to track the status of each operation
    def read_file_contents(self):
        contents = {}
        try:
            with open(self.file_path) as c:
                contents = json.loads(c.read())
        except IOError as e:
            LOGGER.error("Unable to open file due to %s", e)
        return contents

    # Check dataset creation whether it exists or not
    def check_dataset(self):
        url = "%s" % (self.data_service_ip + '/api/v1/datasets')
        try:
            response = requests.get(url)
            if response.status_code is 200:
                LOGGER.debug("Response for check_dataset API is successful with status 200")
                response_dict = json.loads(response.text)
                data_list = response_dict.get('data', [])
                if data_list:
                    for item in data_list:
                        # Checking whether dataset name matches from the list of id's
                        if item['id'] == 'testbot':
                            LOGGER.debug("testbot exists in the list of datasets")
                            dataset_exists = True
                            return dataset_exists
                LOGGER.warning("Dataset doesn't exist in the whole list")
                dataset_exists = False
            else:
                LOGGER.warning("Response for check_dataset API isn't successful")
                dataset_exists = False
        except Exception as e:
            LOGGER.error("Exception while running check dataset function due to %s", e)
            dataset_exists = False
        return dataset_exists

    # Get NAME_NODE_IP to check HDFS directory/file status
    def get_name_node(self):
        try:
            # Using socket module to get IP address of NAME NODE from given hostname
            name_node_ip = socket.gethostbyname(self.cluster_name + '-' + 'hadoop-mgr-1')
            name_node_url = 'http://' + name_node_ip + ':50070' + '/webhdfs/v1/user/pnda?user.name=pnda&' \
                                                                  'op=GETFILESTATUS'
            name_node_response = requests.get(name_node_url)
            if name_node_response.status_code == 200:
                self.hdfs_url = 'http://' + name_node_ip + ':50070'
                name_node = True
                LOGGER.debug("Found Ip address of name_node(hadoop-mgr-1) where webhdfs is running")
            # Get second NAME_NODE IP address if the response from the first NAME NODE is 403
            elif name_node_response.status_code == 403:
                name_node_ip = socket.gethostbyname(self.cluster_name + '-' + 'hadoop-mgr-2')
                name_node_url = 'http://' + name_node_ip + ':50070' + '/webhdfs/v1/user/pnda?user.name=pnda&' \
                                                                      'op=GETFILESTATUS'
                new_node_response = requests.get(name_node_url)
                if new_node_response.status_code is 200:
                    self.hdfs_url = 'http://' + name_node_ip + ':50070'
                    name_node = True
                    LOGGER.debug("Found Ip address of name_node(hadoop-mgr-2) where webhdfs is running")
                else:
                    name_node = False
                    LOGGER.warning("Couldn't get any response from hadoop-mgr-2 node")
            else:
                name_node = False
                LOGGER.warning("Couldn't get any response from hadoop-mgr-1 node")
        except Exception as e:
            LOGGER.error("Exception while executing create_name_node_url due to %s", e)
            name_node = False
        return name_node

    # Check response of an HTTP request and return response content only if response is 200
    def check_dir(self, url):
        status_response = None
        try:
            response = requests.get(url)
            if response.status_code is 200:
                LOGGER.debug("Response is successful from check_dir API")
                status_response = response
            else:
                LOGGER.warning("Response isn't successful from check_dir API")
        except Exception as e:
            LOGGER.error("Exception while executing check_dir function due to %s" % e)
        return status_response

    # Check whether file has been modified recently with in the given time
    def get_latest_modified_file(self, dataset_data):
        avro_file_name = ''
        try:
            response_dict = json.loads(dataset_data.text)
            file_data = response_dict['FileStatuses']['FileStatus']
            if file_data:
                LOGGER.debug("Found file data regarding the given dataset")
                for avro_file in file_data:
                    # Calculating current_time, cron interval time in milliseconds
                    current_milli_time = int(time.time() * 1000)
                    ref_time = int(current_milli_time - (self.cron_interval * 60000))
                    modified_milli_time = avro_file.get('modificationTime')

                    # Checking whether the modified time lies in between before_time, now_time.
                    if modified_milli_time in range(ref_time, current_milli_time):
                        LOGGER.debug("File has been modified recently with in last cron interval time")
                        avro_file_name = avro_file.get('pathSuffix')
                        return avro_file_name
            else:
                LOGGER.warning("Didn't get any data related to the above file path")
        except Exception as e:
            LOGGER.error("Checking HDFS modified time status failed due to exception %s" % e)
        return avro_file_name

    # Check dataset directory to see whether file has been updated with latest data and also if data matches
    def check_dataset_dir(self):
        hdfs_data_matched = False
        try:
            # Getting current time in datetime format and extracting each variable from it to form a directory path.
            current_time = datetime.datetime.now()

            # Adding leading zeros if the value is single digit to match the format of the directory path.
            self.now_year = current_time.year
            self.now_month = "%02d" % (current_time.month,)
            self.now_day = "%02d" % (current_time.day,)
            self.now_hour = "%02d" % (current_time.hour,)

            # Creating the whole url path by combining all the variables fetched above.
            url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/year=%s/month=%s/day=%s'
                                   '/hour=%s?user.name=pnda&op=LISTSTATUS') % \
                  (self.now_year, self.now_month, self.now_day, self.now_hour)
            # Checking whether the above url path exists or not.
            data = self.check_dir(url)
            if data:
                LOGGER.debug("Url path exists")
                # Checking whether the above url path file has been recently modified or not.
                file_name = self.get_latest_modified_file(data)
                if file_name:
                    # Open and read file to check if the data matches with the produced data
                    file_url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/year=%s'
                                                '/month=%s/day=%s/hour=%s/%s?user.name=pnda&op=OPEN') % \
                               (self.now_year, self.now_month, self.now_day, self.now_hour, file_name)
                    file_response = self.check_dir(file_url)
                    if file_response:
                        # Check if unique string 'avrodata' exists in the file response
                        if 'avrodata' in file_response.content:
                            hdfs_data_matched = True
            else:
                # If the above url path doesn't exist, we create a new url path with hour before datetime.
                #  Whenever hour changes gobblin is writing it's newly arrived data in the previous hour file.
                LOGGER.warning("Url path doesn't exist so trying to create a new one with hour before datetime")

                # Getting one hour before time from the current time
                hour_before = datetime.datetime.now() - datetime.timedelta(hours=1)

                # Adding leading zeros if the value is single digit to match the format of the directory path.
                self.before_year = hour_before.year
                self.before_month = "%02d" % (hour_before.month,)
                self.before_day = "%02d" % (hour_before.day,)
                self.before_hour = "%02d" % (hour_before.hour,)

                # Creating an hour_before_url to check whether this url exists or not.
                hour_before_url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/year=%s'
                                                   '/month=%s/day=%s/hour=%s?user.name=pnda&op=LISTSTATUS') % \
                                  (self.before_year, self.before_month, self.before_day, self.before_hour)

                # Checking whether the new hour_before url exists or not.
                new_data = self.check_dir(hour_before_url)
                if new_data:
                    LOGGER.debug("Hour_before url path exists")
                    # Checking whether the new url path file has been modified recently or not.
                    file_name = self.get_latest_modified_file(new_data)
                    # Open and read a file to check if data matches
                    if file_name:
                        file_url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/'
                                                    'year=%s/month=%s/day=%s/hour=%s/%s?user.name=pnda&op=OPEN') % \
                                   (self.before_year, self.before_month, self.before_day, self.before_hour, file_name)
                        # Check if unique string 'avrodata' matches with the response data
                        file_response = self.check_dir(file_url)
                        if file_response:
                            if 'avrodata' in file_response.content:
                                hdfs_data_matched = True
                else:
                    LOGGER.warning("Hour_before url path doesn't exists")
        except Exception as e:
            LOGGER.error("Exception while executing hdfs_status function due to %s", e)
        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'hdfs_dataset.health', [], hdfs_data_matched))
        return hdfs_data_matched

    # Checking the status of HDFS quarantine directory by calling check_dir and check_modified_time functions.
    def check_quarantine_dir(self):
        quar_data_matched = False
        try:
            # Forming quarantine url using the same datetime variables which have been used to check hdfs_status.
            quar_url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/source_topic=%s/year=%s/'
                                        'month=%s/day=%s?user.name=pnda&op=LISTSTATUS') % \
                       (self.runtesttopic, self.now_year, self.now_month, self.now_day)

            # Checking whether the url path exists
            data = self.check_dir(quar_url)
            if data:
                LOGGER.debug("Quarantine Url path exists")
                # Checking whether the above url path file has been recently modified or not.
                quar_file_name = self.get_latest_modified_file(data)
                if quar_file_name:
                    # Open and read quarantine files to check if data matches with produced data
                    quar_file_url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/source_topic=%s/'
                                                     'year=%s/month=%s/day=%s/%s?user.name=pnda&op=OPEN') % \
                                    (self.runtesttopic, self.now_year, self.now_month, self.now_day, quar_file_name)
                    quar_file_response = self.check_dir(quar_file_url)
                    if quar_file_response:
                        # Check if unique string 'quardata' matches with the quarantine file response
                        if 'quardata' in quar_file_response.content:
                            quar_data_matched = True
            else:
                # Since the above url doesn't exist, creating a new url with hour before time.
                day_before_url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/source_topic=%s/'
                                                  'year=%s/month=%s/day=%s?user.name=pnda&op=LISTSTATUS') % \
                                 (self.runtesttopic, self.before_year, self.before_month, self.before_day)

                # Checking whether the new day before quarantine url exists or not.
                new_data = self.check_dir(day_before_url)
                if new_data:
                    LOGGER.debug("Day before Quarantine url path exists")
                    # Checking whether the above url path file has been recently modified or not.
                    quar_file_name = self.get_latest_modified_file(new_data)
                    if quar_file_name:
                        # Open and read quarantine file to check if data matches
                        quar_file_url = (self.hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/'
                                                         'source_topic=%s/year=%s/month=%s/day=%s/%s?user.name=pnda&'
                                                         'op=OPEN') % (self.runtesttopic, self.before_year,
                                                                       self.before_month, self.before_day,
                                                                       quar_file_name)
                        quar_file_response = self.check_dir(quar_file_url)
                        if quar_file_response:
                            # Check if unique string 'quardata' matches with the quarantine file response
                            if 'quardata' in quar_file_response.content:
                                quar_data_matched = True
                else:
                    LOGGER.warning("Day before Quarantine url path doesn't exists")
        except Exception as e:
            LOGGER.error("Exception while executing quarantine_status due to %s", e)
        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'quarantine_dataset.health', [], quar_data_matched))
        return quar_data_matched

    # Run whole dataset flow to check dataset content stored in HDFS filesystem
    def check_dataset_content(self):
        dataset_check = False
        try:
            if self.check_dataset():
                if self.get_name_node():
                    dataset_status = self.check_dataset_dir()
                    quar_status = self.check_quarantine_dir()
                    if dataset_status and quar_status:
                        dataset_check = True
        except Exception as e:
            LOGGER.error("Exception while checking dataset content due to %s", e)
        return dataset_check

    def exec_test(self):
        try:
            # Create a status file if not present to track status of each health check
            if not os.path.isfile(self.file_path):
                status_dict = {}
                status_dict.update({'first-run-gobblin-wait': 1, 'error-reported': 0, 'retries': 0,
                                    'lastlogtimestamp': ''})
                with open(self.file_path, 'w') as f:
                    f.write(json.dumps(status_dict))

            # Read file contents if file is already present
            file_data = self.read_file_contents()
            first_run_gobblin_wait = file_data.get('first-run-gobblin-wait', 1)
            retry_count = file_data.get('retries', 0)
            lastlogtimestamp = file_data.get('lastlogtimestamp', '')
            error_report = file_data.get('error-reported', 0)

            # Check Kafka health before proceeding to gobblin and dataset health check
            kafka_health = self.get_kafka_health()
            if kafka_health:
                # Initial Gobblin health check(Wait until it finishes it's max retries)
                if first_run_gobblin_wait:
                    # Query Elasticsearch to check if there is any gobblin log generated
                    gobblin_log_status = self.query_es()
                    if gobblin_log_status:
                        # Extract timestamp of the log file from the hits list
                        logtimestamp = gobblin_log_status[0]['_source']['@timestamp']
                        # Update status dict with newly generated timestamp
                        updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 0, 'retries': 0,
                                        'lastlogtimestamp': logtimestamp}
                        # Save updated status dict to status file
                        self.save_status_vars(updated_dict)
                        # Report initial gobblin health metric
                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'initial_gobblin.health',
                                                  [], "OK"))
                        # Check HDFS filesystem to see if data exists and also if content matches.
                        dataset_status = self.check_dataset_content()
                        if dataset_status:
                            # Report health metric of initial dataset health check
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'initial_dataset.health',
                                                      [], "OK"))
                        else:
                            cause = "Dataset check failed initially"
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'initial_dataset.health',
                                                      [cause], "ERROR"))
                    # Retry querying elasticsearch until it meets the max retries count
                    else:
                        retry_count = retry_count + 1
                        # Check if retry count exceeded the max number of retries
                        if retry_count >= self.num_attempts:
                            # Update status flag "first-run-gobblin-wait" to 0 which notifies that initial check is done
                            updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 0, 'retries': 0,
                                            'lastlogtimestamp': ''}
                            # Save updated dict to status file
                            self.save_status_vars(updated_dict)
                            # Report health metric "initial gobblin health" as ERROR
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'initial_gobblin.health',
                                                  [], "ERROR"))
                    return
                # Enters only if status flag "first_run_gobblin_wait" is 0(Either initial gobblin check is OK or it
                # exceeded max retry count.
                else:
                    # Query Elasticsearch to check if there is any new generated log
                    gobblin_log_status = self.query_es()
                    if gobblin_log_status:
                        # Extract timestamp from the captured log
                        logtimestamp = gobblin_log_status[0]['_source']['@timestamp']
                        # Check if previous log timestamp and current logtimestamp are same
                        if lastlogtimestamp == logtimestamp:
                            # Report error(gobblin health) only if isn't reported previously(When timestamps are same)
                            if not error_report:
                                # Check if current log timestamp has crossed the cron interval specified.
                                ref_time = datetime.datetime.now() - datetime.timedelta(minutes=self.cron_interval)
                                logtimestamp = datetime.datetime(*map(int, re.split('[^\d]', logtimestamp)[:-1]))
                                '''
                                If the current log timestamp has crossed it's cron interval and still there is no new
                                log that has been generated, give few retry attempts to check if there is any new log.
                                '''
                                if (logtimestamp - ref_time).total_seconds() < 0:
                                    retry_count = retry_count + 1
                                    # Check if it has exceeded maximum retries to report error.
                                    if retry_count >= self.num_attempts:
                                        cause = "Didn't receive new log even after maximum retries"
                                        # Report health metric gobblin health as error
                                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset',
                                                                  'gobblin.health', [cause], "ERROR"))
                                        # Update status flags "error-reported" to 1
                                        updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 1, 'retries': 0,
                                                        'lastlogtimestamp': lastlogtimestamp}
                                        # Save updated variables to status file
                                        self.save_status_vars(updated_dict)
                        # Enters if there is a new log that has been generated.
                        else:
                            # Update status flags to 0 and lastlogtimestamp to newly generated log timestamp.
                            updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 0, 'retries': 0,
                                            'lastlogtimestamp': logtimestamp}
                            self.save_status_vars(updated_dict)
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'gobblin.health',
                                                      [], "OK"))
                            # Check dataset content to see if data exists and also content matches
                            dataset_status = self.check_dataset_content()
                            if dataset_status:
                                self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'dataset.health', [],
                                                          "OK"))
                            else:
                                cause = "Dataset check failed"
                                self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'dataset.health',
                                                          [cause], "ERROR"))
                    # Didn't get any response from Elasticsearch related to gobblin logs.
                    else:
                        cause = "Error reading gobblin log for ELK"
                        # Report Gobblin and dataset health metrics as "UNAVAILABLE"
                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'gobblin.health',[cause],
                                                  "UNAVAILABLE"))
                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'dataset.health', [cause],
                                                  "UNAVAILABLE"))
        except Exception as e:
            LOGGER.error("Exception while executing whole test flow due to %s", e)

    def runner(self, args, display=True):
        """
        Main section.
        """
        plugin_args = args.split() \
            if args is not None and (len(args.strip()) > 0) \
            else ""

        options = self.read_args(plugin_args)
        self.data_service_ip = options.data_service.split(",")[0]
        self.cluster_name = options.cluster_name.split(",")[0]
        self.elk_ip = options.elk_ip.split(",")[0]
        self.cron_interval = int(options.cron_interval)
        self.num_attempts = int(options.num_attempts)
        self.gobblin_log_path = options.gobblin_log_path
        self.metric_console = options.metric_console.split(",")[0]

        # Form elasticsearch url and query to check if there is any gobblin related log from es response.
        self.uri = self.elk_ip + "/logstash-*/_search?pretty=true"
        self.query = '{"sort":[{"@timestamp":{"order":"desc"}}], "query":{"bool":{"must":[{"match":' \
                     '{"source":"gobblin"}}, {"match":{"path": "%s"}}, ' \
                     '{"match_phrase":{"message":"Shutting down the application"}}]}},"size":1}' % self.gobblin_log_path
        self.exec_test()
        if display:
            self._do_display(self.results)
        return self.results
