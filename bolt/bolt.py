#!/usr/bin/env python

import SocketServer
import threading
import sys
import time

from bolt_broker import MajordomoBroker
from bolt_discovery import UDPReceivedHandler

def main(): 
	broker_server = MajordomoBroker()
	discovery_server = SocketServer.UDPServer(("0.0.0.0", 5555), UDPReceivedHandler)

	broker_server_thread = threading.Thread(target=broker_server.serve_forever)
	discovery_server_thread = threading.Thread(target=discovery_server.serve_forever)

	broker_server_thread.daemon = True
	discovery_server_thread.daemon = True

	exit_status = 1

	try:
		broker_server_thread.start()
		discovery_server_thread.start()

		while True:
			time.sleep(100)
	except Exception as e:
		sys.stderr.write(str(e))
	except KeyboardInterrupt as e:
		sys.stdout.write("Exit by user\n")
		exit_status = 0
	#finally:
		#if broker_server is not None:
		#	broker_server.terminate()
		#if discovery_server is not None:
		#	discovery_server.terminate()

	return exit_status

if __name__ == '__main__':
	ret = main()
	sys.exit(ret)
