#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import logging
import sh
import sys
import argparse

from sh import docker


# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)


version="1.0"
prog_name = sys.argv[0]
parser = argparse.ArgumentParser(prog="{pn} {v}".format(pn=prog_name, v=version))
usage = """{pn} [options]
Script to copy the fbs file from the flaki container and generate the flatbuffer code. 
   container_name : name of the flaki container
""".format(
    pn=prog_name
)

parser.add_argument(
    '--container-name',
    dest="container_name",
    help='Name of the flaki container: Ex : flaki-service',
    required=True
)

parser.add_argument(
    '--debug',
    dest="debug",
    default=False,
    action="store_true",
    help='Enable debug'
)



if __name__ == "__main__":

    # Debug
    args = parser.parse_args()
    debug = args.debug
    logger = logging.getLogger("flaki_tools.generate_flatbuffer")
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


    container_name = args.container_name
    fbs_file = "flaki.fbs"
    path_file = "/cloudtrust/flaki-service/pkg/flaki/flatbuffer/"
    path_directory = "./tests/flatbuffer/"
    url = "127.0.0.1:8888"

    # obtain the .fbs files used by the flaki container
    sh.mkdir(path_directory)
    get_fbs_file = docker.bake("cp", container_name+":" + path_file + fbs_file, path_directory)
    logger.debug(get_fbs_file)
    get_fbs_file()
    # flatc --python
    sh.flatc("--python", "-o", path_directory, path_directory + fbs_file)
    logger.debug("flatc --python -o {output} {file}".format(output=path_directory, file=path_directory+fbs_file))
    logger.info("Copied the .fbs file and generated the code for serialization")
