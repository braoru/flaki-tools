#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import re
import pytest
import logging
import json
import jsonschema

from requests import Request, Session
from helpers.logging import prepared_request_to_json

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("influx_tools.tests.test_flaki_container")
logger.setLevel(logging.INFO)

@pytest.fixture()
def default_route_json_schema():
    schema = {
        "type": "object",
        "$schema": "http://json-schema.org/schema#",
        "properties": {
            "name": {
                "type": "string"
            },
            "version": {
                "$id": "/properties/version",
                "type": "string"
            },
            "environment": {
                "type": "string"
            },
            "commit": {
                "type": "string"
            }
        },
    }
    return schema

@pytest.fixture()
def services_json_schema():
    schema = {
        "type": "object",
        "$schema": "http://json-schema.org/schema#",
        "required": ["influx", "jaeger", "keycloak", "redis", "sentry"],
        "properties": {
            "influx": {
                "type": "string"
            },
            "jaeger": {
                "type": "string"
            },
            "redis": {
                "type": "string"
            },
            "sentry":{
                "type": "string"
            }
        },
    }
    return schema

@pytest.mark.usefixtures('settings', scope='class')
@pytest.mark.usefixtures('default_route_json_schema', scope='classe')
@pytest.mark.usefixtures('services_json_schema', scope='classe')
class TestContainerFlaki():
    """
        Class to test the flaki service.
    """

    def test_is_flaki_online(self, settings, default_route_json_schema):
        """
        Test if flaki service is running
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        # Challenge value
        component_name = settings['flaki']['component_name']

        # Settings
        flaki_scheme = settings['flaki']['http_scheme']
        flaki_ip = settings['flaki']['ip']
        flaki_port = settings['flaki']['port']

        # Test
        s = Session()

        headers = {
            'Accept': "application/json'",
        }

        req = Request(
            method='GET',
            url="{scheme}://{ip}:{port}/".format(
                scheme=flaki_scheme,
                ip=flaki_ip,
                port=flaki_port
            ),
            headers=headers
        )

        prepared_request = req.prepare()

        logger.debug(
            json.dumps(
                prepared_request_to_json(req),
                sort_keys=True,
                indent=4,
                separators=(',', ': ')
            )
        )

        response = s.send(prepared_request, verify=False)
        response_json = response.json()

        assert jsonschema.Draft3Validator(default_route_json_schema).is_valid(response_json)

        assert re.search(
            component_name,
            response_json['name']
        )

    def test_flaki_health_checks(self, settings, default_route_json_schema, services_json_schema):
        """
        Flaki service offers the possibility to do health checks on the services that work with it. This test launches the
        health checks and sees that all services are up
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        # Challenge value
        fail_value = "KO"

        # Settings
        flaki_scheme = settings['flaki']['http_scheme']
        flaki_ip = settings['flaki']['ip']
        flaki_port = settings['flaki']['port']

        # Test
        s = Session()

        headers = {
            'Accept': "application/json'",
        }

        req = Request(
            method='GET',
            url="{scheme}://{ip}:{port}/health".format(
                scheme=flaki_scheme,
                ip=flaki_ip,
                port=flaki_port
            ),
            headers=headers
        )

        prepared_request = req.prepare()

        logger.debug(
            json.dumps(
                prepared_request_to_json(req),
                sort_keys=True,
                indent=4,
                separators=(',', ': ')
            )
        )

        response = s.send(prepared_request, verify=False)
        response_json = response.json()

        assert jsonschema.Draft3Validator(services_json_schema).is_valid(response_json)

        assert re.search(
            fail_value,
            response.text
        ) is None
