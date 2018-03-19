# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import pytest
import json
import sh

from sh import docker

def pytest_addoption(parser):
    parser.addoption("--cassandra-cred", action="store", help="Json containing credentials for cassandra",
                     dest="cassandra_cred")
    parser.addoption("--config-file", action="store", help="Json container configuration file ", dest="config_file")
    parser.addoption("--influx-cred", action="store", help="Json containing credentials for influxdb", dest="influxdb_cred")


@pytest.fixture()
def settings(pytestconfig):
	try:
		with open(pytestconfig.getoption('config_file')) as json_data:
			config = json.load(json_data)

	except IOError as e:
		raise IOError("Config file {path} not found".format(path=pytestconfig.getoption('config_file')))

	return config


@pytest.fixture()
def influxdb_cred(pytestconfig):
	try:
		with open(pytestconfig.getoption('influxdb_cred')) as json_data:
			cred = json.load(json_data)

	except IOError as e:
		raise IOError("Influxdb credentials file {path} not found".format(path=pytestconfig.getoption('influxdb_cred')))

	return cred

@pytest.fixture()
def cassandra_cred(pytestconfig):
	try:
		with open(pytestconfig.getoption('cassandra_cred')) as json_data:
			cred = json.load(json_data)

	except IOError as e:
		raise IOError("Cassandra credentials file {path} not found".format(path=pytestconfig.getoption('cassandra_cred')))

	return cred
