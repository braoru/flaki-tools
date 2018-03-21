#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import pytest
import logging
import flatbuffers.python.flatbuffers as flatbuffers
#import flatbuffers
import http.client
import time
import json
import grpc

from flatbuffer.fb import FlakiReply as fresp
from flatbuffer.fb import FlakiRequest as freq
from flatbuffer.fb import flaki_grpc_fb as fgrpc
#from flaki_pb2 import flaki_pb2_grpc as fgrpc
#from flaki_pb2 import flaki_pb2 as fpb2
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


def cassandra_check(self, cassandra_cred, correlation_id):
    cassandra_user = cassandra_cred.get('user')
    cassandra_password = cassandra_cred.get('password')
    cassandra_keyspace = cassandra_cred.get('keyspace')

    logger.info("Connecting to Cassandra db with username {user} on keyspace {keyspace}".format(user=cassandra_user,
                                                                                                keyspace=cassandra_keyspace))
    auth_provider = PlainTextAuthProvider(username=cassandra_user, password=cassandra_password)
    cluster = Cluster(auth_provider=auth_provider)
    session = cluster.connect(cassandra_keyspace)
    res = session.execute("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                          "FILTERING;".format(id=correlation_id))
    logger.debug("SELECT * FROM tag_index WHERE tag_value='{id}' AND tag_key='correlation_id'  ALLOW "
                 "FILTERING;".format(id=correlation_id))
    return res.current_rows

def influx_check(self, influxdb_cred, correlation_id, skip_table):
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
            if table['name'] != skip_table:
                query = "SELECT * FROM {name} WHERE correlation_id='{id}';".format(name=table['name'],
                                                                                       id=correlation_id)
                res = client.query(query)
                logger.debug(query)
                no_entries = len(list(res.get_points(tags={"correlation_id": correlation_id})))

    except Exception as e:
        logger.debug(e)
        raise e
    finally:
        client.close()
        logger.info("Influxdb client: closed HTTP session")

    return no_entries


def jaeger_check(self, correlation_id, jaeger_url, url_params):
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
    return found_corr_id

def http_flaki_request_nextid(self, url, correlation_id, method):

    b = flatbuffers.Builder(0)
    freq.FlakiRequestStart(b)
    b.Finish(freq.FlakiRequestEnd(b))

    if correlation_id != "-1":
        headers = {"Content-Type": "application/octet-stream", "X-Correlation-ID": correlation_id}
        logger.info("POST request to flaki with correlation id {id}".format(id=correlation_id))
    else:
        headers = {"Content-Type": "application/octet-stream"}
        logger.info("POST request to flaki without any prior correlation id")

    conn = http.client.HTTPConnection(url)
    conn.request("POST", method, b.Output(), headers)

    resp = conn.getresponse()
    data = fresp.FlakiReply().GetRootAsFlakiReply(resp.read(), 0)
    new_correlation_id = data.Id().decode("utf-8")
    logger.info("Flaki next id response is {id}".format(id=new_correlation_id))
    conn.close()

    return new_correlation_id

@pytest.mark.usefixtures('settings', 'influxdb_cred', 'cassandra_cred', scope='class')
class TestContainerFlaki():
    """
        Class to perform acceptance test of the flaki service.
        Once a request is done to flaki service, with a correlation id, the id must be found also in cassandra and influx.
    """

    def test_http_flaki_nextid_with_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        # correlation id used for testing
        correlation_id = "9876"
        url = "127.0.0.1:8888"
        method = "/nextid"
        new_correlation_id = http_flaki_request_nextid(self, url, correlation_id, method)

        time.sleep(2)

        # Cassandra
        ## check that we have the correlation_id in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influx
        # check that we have metrics with the correlation id
        skip_table = "nextvalidid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3

    def test_http_flaki_nextvalidid_with_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        # correlation id used for testing
        correlation_id = "9879"
        url = "127.0.0.1:8888"
        method = "/nextvalidid"

        new_correlation_id = http_flaki_request_nextid(self, url, correlation_id, method)

        time.sleep(2)

        # Cassandra
        # check that we have the correlation_id in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influxdb
        # check that we have metrics with the correlation id
        skip_table = "nextid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3

    def test_http_flaki_nextvalidid_without_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        # we use the value of -1 to denote that we don't have any correlation when calling Flaki service
        no_correlation_id = "-1"
        url = "127.0.0.1:8888"
        method = "/nextvalidid"

        correlation_id = http_flaki_request_nextid(self, url, no_correlation_id, method)

        time.sleep(2)

        # Cassandra
        # check that we have the correlation_id obtained from Flaki in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influxdb
        # check that we have metrics with the correlation id obtained from Flaki
        skip_table = "nextid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id  obtained from Flaki is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3

    def test_http_flaki_nextid_without_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        # we use the value of -1 to denote that we don't have any correlation when calling Flaki service
        no_correlation_id = "-1"
        url = "127.0.0.1:8888"
        method = "/nextid"

        correlation_id = http_flaki_request_nextid(self, url, no_correlation_id, method)

        time.sleep(2)

        # Cassandra
        # check that we have the correlation_id obtained from Flaki in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influxdb
        # check that we have metrics with the correlation id obtained from Flaki
        skip_table = "nextvalidid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id  obtained from Flaki is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3

    def test_grpc_flaki_nextid_without_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        url = "127.0.0.1:5555"

        b = flatbuffers.Builder(0)
        freq.FlakiRequestStart(b)
        b.Finish(freq.FlakiRequestEnd(b))

        # GRPC call
        channel = grpc.insecure_channel(url)
        stub = fgrpc.FlakiStub(channel)
        res = stub.NextID(bytes(b.Output()))
        data = fresp.FlakiReply().GetRootAsFlakiReply(res, 0)
        correlation_id = data.Id().decode("utf-8")

        time.sleep(2)

        # Cassandra
        # check that we have the correlation_id obtained from Flaki in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influxdb
        # check that we have metrics with the correlation id obtained from Flaki
        skip_table = "nextvalidid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id  obtained from Flaki is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3


    def test_grpc_flaki_nextid_with_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        container_name = settings['container_name']
        correlation_id = "123"

        url = "127.0.0.1:5555"
        b = flatbuffers.Builder(0)
        freq.FlakiRequestStart(b)
        b.Finish(freq.FlakiRequestEnd(b))

        # GRPC call
        channel = grpc.insecure_channel(url)
        stub = fgrpc.FlakiStub(channel)
        metadata = [('correlation_id', correlation_id)]
        res = stub.NextID(bytes(b.Output()), metadata=metadata,)
        data = fresp.FlakiReply().GetRootAsFlakiReply(res, 0)
        new_correlation_id = data.Id().decode("utf-8")

        time.sleep(2)

        # Cassandra
        # check that we have the correlation_id is in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influxdb
        # check that we have metrics with the correlation id obtained from Flaki
        skip_table = "nextvalidid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id  obtained from Flaki is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3

    def test_grpc_flaki_nextvalidid_without_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        url = "127.0.0.1:5555"

        b = flatbuffers.Builder(0)
        freq.FlakiRequestStart(b)
        b.Finish(freq.FlakiRequestEnd(b))

        # GRPC call
        channel = grpc.insecure_channel(url)
        stub = fgrpc.FlakiStub(channel)
        res = stub.NextValidID(bytes(b.Output()))
        data = fresp.FlakiReply().GetRootAsFlakiReply(res, 0)
        correlation_id = data.Id().decode("utf-8")

        time.sleep(2)

        # Cassandra
        # check that we have the correlation_id obtained from Flaki in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influxdb
        # check that we have metrics with the correlation id obtained from Flaki
        skip_table = "nextid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id  obtained from Flaki is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3

    def test_grpc_flaki_nextvalidid_with_correlation_id(self, settings, influxdb_cred, cassandra_cred):

        container_name = settings['container_name']
        correlation_id = "123"

        url = "127.0.0.1:5555"
        b = flatbuffers.Builder(0)
        freq.FlakiRequestStart(b)
        b.Finish(freq.FlakiRequestEnd(b))

        # GRPC call
        channel = grpc.insecure_channel(url)
        stub = fgrpc.FlakiStub(channel)
        metadata = [('correlation_id', correlation_id)]
        res = stub.NextValidID(bytes(b.Output()), metadata=metadata,)
        data = fresp.FlakiReply().GetRootAsFlakiReply(res, 0)
        new_correlation_id = data.Id().decode("utf-8")

        time.sleep(2)

        # Cassandra
        # check that we have the correlation_id is in the db
        no_entries = len(cassandra_check(self, cassandra_cred, correlation_id))
        assert no_entries >= 1

        # Influxdb
        # check that we have metrics with the correlation id obtained from Flaki
        skip_table = "nextid_endpoint"

        no_entries = influx_check(self, influxdb_cred, correlation_id, skip_table)
        assert no_entries >= 1

        # Jaeger UI
        # check that the correlation id  obtained from Flaki is a tag in the spans
        service_name = "flaki-service"
        jaeger_url = "127.0.0.1:16686"
        url_params = "/api/traces?service={service}&tag=correlation_id:{id}".format(service=service_name,
                                                                                    id=correlation_id)

        found_corr_id = jaeger_check(self, correlation_id, jaeger_url, url_params)
        assert found_corr_id >= 3