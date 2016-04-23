#!/usr/bin/env python

import SocketServer

class UDPReceivedHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		data = self.request[0].strip()
		socket = self.request[1]

		if data == "BDP01REQ":
			print self.client_address
			socket.sendto("BDP01REP", self.client_address)
		else:
			print data
        
if __name__ == "__main__":
	try:
		HOST, PORT = "0.0.0.0", 5555
		server = SocketServer.UDPServer((HOST, PORT), UDPReceivedHandler)
		server.serve_forever()
	except KeyboardInterrupt as e:
		pass

