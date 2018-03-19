#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import pytest
import logging
import flatbuffers
import http.client
import time
import json

from flatbuffer.fb import FlakiReply as fresp
from flatbuffer.fb import FlakiRequest as freq
from influxdb import InfluxDBClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("influx_tools.tests.test_influx_container")
logger.setLevel(logging.INFO)


@pytest.mark.usefixtures('settings', 'influxdb_cred', 'cassandra_cred', scope='class')
class TestContainerFlaki():
    """
        Class to perform acceptance test of the flaki service.
        Once a request is done to flaki service, with a correlation id, the id must be found also in cassandra and influx.
    """

    def test_http_flaki_nextid_with_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        # correlation id used for testing
        correlation_id = "9876"

        # construct the http request
        url = "127.0.0.1:8888"
        b = flatbuffers.Builder(0)
        freq.FlakiRequestStart(b)
        b.Finish(freq.FlakiRequestEnd(b))
        headers = {"Content-Type": "application/octet-stream", "X-Correlation-ID": correlation_id}
        conn = http.client.HTTPConnection(url)
        conn.request("POST", "/nextid", b.Output(), headers)
        logger.info("POST request to flaki with correlation id {id}".format(id=correlation_id))
        resp = conn.getresponse()
        data = fresp.FlakiReply().GetRootAsFlakiReply(resp.read(), 0)
        new_correlation_id = data.Id().decode("utf-8")
        logger.info("Flaki next id response is id {id}".format(id=new_correlation_id))
        conn.close()

        time.sleep(2)

        # Cassandra
        cassandra_user = cassandra_cred.get('user')
        cassandra_password = cassandra_cred.get('password')
        cassandra_keyspace = cassandra_cred.get('keyspace')

        logger.info("Connecting to Cassandra db with username {user} on keyspace {keyspace}".format(user=cassandra_user, keyspace=cassandra_keyspace))
        auth_provider = PlainTextAuthProvider(username=cassandra_user, password=cassandra_password)
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect(cassandra_keyspace)
        res = session.execute("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                              "FILTERING;".format(id=correlation_id))
        logger.debug("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                              "FILTERING;".format(id=correlation_id))
        no_entries = len(res.current_rows)
        assert no_entries >= 1

        # Influxdb
        influxdb_user = influxdb_cred.get('user')
        influxdb_password = influxdb_cred.get('password')
        influxdb_db = influxdb_cred.get('db_name')

        try:
            client = InfluxDBClient(username=influxdb_user, password=influxdb_password, database=influxdb_db)
            logger.info("Connecting to influxdb with user {user}".format(user=influxdb_user))
        except Exception as e:
            logger.debug(e)
            raise e

        try:
            measurements = client.get_list_measurements()
            logger.info("Checking that the correlation id {id} appears in every measurement of the database {db}".
                        format(id=correlation_id, db=influxdb_db))
            for table in measurements:
                if table['name'] != "nextvalidid_endpoint":
                    query = "SELECT * FROM {name} WHERE correlation_id='{id}';".format(name=table['name'], id=correlation_id)
                    res = client.query(query)
                    logger.debug(query)
                    no_entries = len(list(res.get_points(tags={"correlation_id": correlation_id})))
                    assert no_entries >= 1
        except Exception as e:
            logger.debug(e)
            raise e
        finally:
            client.close()
            logger.info("Influxdb client: closed HTTP session")

        # Jaeger UI
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        conn = http.client.HTTPConnection(jaeger_url)
        conn.request("GET", url=url_params)
        logger.debug("GET {url}{params}".format(url=jaeger_url, params=url_params))
        resp = conn.getresponse().read()
        data = json.loads(resp)
        found_corr_id = 0
        logger.info("Checking that the correlation id {id} appears in every span".format(id=correlation_id))
        for data_item in data['data']:
            for span in data_item['spans']:
                for tag in span['tags']:
                    if tag['value'] == correlation_id and tag['key'] == 'correlation_id':
                        found_corr_id += 1
        assert found_corr_id >= 3

    def test_http_flaki_nextvalidid_with_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        #correlation id used for testing
        correlation_id = "9879"

        # construct the http request
        url = "127.0.0.1:8888"
        b = flatbuffers.Builder(0)
        freq.FlakiRequestStart(b)
        b.Finish(freq.FlakiRequestEnd(b))
        headers = {"Content-Type": "application/octet-stream", "X-Correlation-ID": correlation_id}
        conn = http.client.HTTPConnection(url)
        conn.request("POST", "/nextvalidid", b.Output(), headers)
        logger.info("POST request to flaki with correlation id {id}".format(id=correlation_id))
        resp = conn.getresponse()
        data = fresp.FlakiReply().GetRootAsFlakiReply(resp.read(), 0)
        new_correlation_id = data.Id().decode("utf-8")
        logger.info("Flaki next valid id response is {id}".format(id=new_correlation_id))
        conn.close()

        time.sleep(2)

        # Cassandra
        cassandra_user = cassandra_cred.get('user')
        cassandra_password = cassandra_cred.get('password')
        cassandra_keyspace = cassandra_cred.get('keyspace')

        logger.info("Connecting to Cassandra db with username {user} on keyspace {keyspace}".format(user=cassandra_user, keyspace=cassandra_keyspace))
        auth_provider = PlainTextAuthProvider(username=cassandra_user, password=cassandra_password)
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect(cassandra_keyspace)
        res = session.execute("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                              "FILTERING;".format(id=correlation_id))
        logger.debug("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                              "FILTERING;".format(id=correlation_id))
        no_entries = len(res.current_rows)
        assert no_entries >= 1

        # Influxdb
        influxdb_user = influxdb_cred.get('user')
        influxdb_password = influxdb_cred.get('password')
        influxdb_db = influxdb_cred.get('db_name')

        try:
            client = InfluxDBClient(username=influxdb_user, password=influxdb_password, database=influxdb_db)
            logger.info("Connecting to influxdb with user {user}".format(user=influxdb_user))
        except Exception as e:
            logger.debug(e)
            raise e

        try:
            measurements = client.get_list_measurements()
            logger.info("Checking that the correlation id {id} appears in every measurement of the database {db}".
                        format(id=correlation_id, db=influxdb_db))
            for table in measurements:
                if table['name'] != "nextid_endpoint":
                    query = "SELECT * FROM {name} WHERE correlation_id='{id}';".format(name=table['name'], id=correlation_id)
                    res = client.query(query)
                    logger.debug(query)
                    no_entries = len(list(res.get_points(tags={"correlation_id": correlation_id})))
                    assert no_entries >= 1
        except Exception as e:
            logger.debug(e)
            raise e
        finally:
            client.close()
            logger.info("Influxdb client: closed HTTP session")

        # Jaeger UI
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        conn = http.client.HTTPConnection(jaeger_url)
        conn.request("GET", url=url_params)
        logger.debug("GET {url}{params}".format(url=jaeger_url, params=url_params))
        resp = conn.getresponse().read()
        data = json.loads(resp)
        found_corr_id = 0
        logger.info("Checking that the correlation id {id} appears in every span".format(id=correlation_id))
        for data_item in data['data']:
            for span in data_item['spans']:
                for tag in span['tags']:
                    if tag['value'] == correlation_id and tag['key'] == 'correlation_id':
                        found_corr_id += 1
        assert found_corr_id >= 3


    def test_http_flaki_nextvalidid_without_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        # construct the http request
        url = "127.0.0.1:8888"
        b = flatbuffers.Builder(0)
        freq.FlakiRequestStart(b)
        b.Finish(freq.FlakiRequestEnd(b))
        headers = {"Content-Type": "application/octet-stream"}
        conn = http.client.HTTPConnection(url)
        conn.request("POST", "/nextvalidid", b.Output(), headers)
        logger.info("POST request to flaki")
        resp = conn.getresponse()
        data = fresp.FlakiReply().GetRootAsFlakiReply(resp.read(), 0)
        correlation_id = data.Id().decode("utf-8")
        logger.info("Flaki next valid id response is {id}".format(id=correlation_id))
        conn.close()

        time.sleep(2)

        # Cassandra
        cassandra_user = cassandra_cred.get('user')
        cassandra_password = cassandra_cred.get('password')
        cassandra_keyspace = cassandra_cred.get('keyspace')

        logger.info("Connecting to Cassandra db with username {user} on keyspace {keyspace}".format(user=cassandra_user, keyspace=cassandra_keyspace))
        auth_provider = PlainTextAuthProvider(username=cassandra_user, password=cassandra_password)
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect(cassandra_keyspace)
        res = session.execute("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                              "FILTERING;".format(id=correlation_id))
        logger.debug("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                              "FILTERING;".format(id=correlation_id))
        no_entries = len(res.current_rows)
        assert no_entries >= 1

        # Influxdb
        influxdb_user = influxdb_cred.get('user')
        influxdb_password = influxdb_cred.get('password')
        influxdb_db = influxdb_cred.get('db_name')

        try:
            client = InfluxDBClient(username=influxdb_user, password=influxdb_password, database=influxdb_db)
            logger.info("Connecting to influxdb with user {user}".format(user=influxdb_user))
        except Exception as e:
            logger.debug(e)
            raise e

        try:
            measurements = client.get_list_measurements()
            logger.info("Checking that the correlation id {id} appears in every measurement of the database {db}".
                        format(id=correlation_id, db=influxdb_db))
            for table in measurements:
                if table['name'] != "nextid_endpoint":
                    query = "SELECT * FROM {name} WHERE correlation_id='{id}';".format(name=table['name'], id=correlation_id)
                    res = client.query(query)
                    logger.debug(query)
                    no_entries = len(list(res.get_points(tags={"correlation_id": correlation_id})))
                    assert no_entries >= 1
        except Exception as e:
            logger.debug(e)
            raise e
        finally:
            client.close()
            logger.info("Influxdb client: closed HTTP session")

        # Jaeger UI
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        conn = http.client.HTTPConnection(jaeger_url)
        conn.request("GET", url=url_params)
        logger.debug("GET {url}{params}".format(url=jaeger_url, params=url_params))
        resp = conn.getresponse().read()
        data = json.loads(resp)
        found_corr_id = 0
        logger.info("Checking that the correlation id {id} appears in every span".format(id=correlation_id))
        for data_item in data['data']:
            for span in data_item['spans']:
                for tag in span['tags']:
                    if tag['value'] == correlation_id and tag['key'] == 'correlation_id':
                        found_corr_id += 1
        assert found_corr_id >= 3


    # def test_grpc_flaki_nextvalidid_without_correlation_id(self, settings, influxdb_cred, cassandra_cred):
    #
    #     container_name = settings['container_name']
    #
    #     # construct the http request
    #     url = "127.0.0.1:8888"
    #     b = flatbuffers.Builder(0)
    #     freq.FlakiRequestStart(b)
    #     b.Finish(freq.FlakiRequestEnd(b))
    #
    #     channel = grpc.insecure_channel(url)
