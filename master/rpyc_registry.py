#!/usr/bin/env python
"""
The registry server listens to broadcasts on UDP port 18812, answering to
discovery queries by clients and registering keepalives from all running
servers. In order for clients to use discovery, a registry service must
be running somewhere on their local network.
"""

import time
from optparse import OptionParser
from rpyc.utils.registry import REGISTRY_PORT, DEFAULT_PRUNING_TIMEOUT
from rpyc.utils.registry import UDPRegistryServer, TCPRegistryServer
from rpyc.lib import setup_logger

parser = OptionParser()
parser.add_option("-m", "--mode", action="store", dest="mode", metavar="MODE",
    default="udp", type="string", help="mode can be 'udp' or 'tcp'")
parser.add_option("-p", "--port", action="store", dest="port", type="int",
    metavar="PORT", default=REGISTRY_PORT, help="specify a different UDP/TCP listener port")
parser.add_option("-f", "--file", action="store", dest="logfile", type="str",
    metavar="FILE", default=None, help="specify the log file to use; the default is stderr")
parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
    default=False, help="quiet mode (only errors are logged)")
parser.add_option("-t", "--timeout", action="store", dest="pruning_timeout",
    type="int", default=61, help="sets a custom pruning timeout")

def main():
    options, args = parser.parse_args()
    server = TCPRegistryServer(port=18811, pruning_timeout=61)
    setup_logger(options)
    server.start()


if __name__ == "__main__":
    main()
