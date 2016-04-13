#!/usr/bin/env python

"""
 Majordomo Protocol Client API, Python version.

 Implements the MDP/Worker spec at http:#rfc.zeromq.org/spec:7.

 Modified from python examples by Min RK, <benjaminrk@gmail.com>
"""

import logging
import zmq
import socket
import sys

import MDP
from zhelpers import dump

class MajordomoClient(object):
	"""
	Majordomo Protocol Client API, Python version.
	Implements the MDP/Worker spec at http:#rfc.zeromq.org/spec:7.
	"""
	broker = None
	ctx = None
	client = None
	poller = None
	timeout = 2500
	verbose = False

	def __init__(self, verbose=False):
		self.verbose = verbose
		self.ctx = zmq.Context()
		self.poller = zmq.Poller()
		logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO)
		self.get_broker()
		self.reconnect_to_broker()
	
	def udp_send(self,data,ip,port):
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.sendto(data + "\n", (ip, port))

			sock.settimeout(1.0)
			return sock.recv(1024)
		except:
			return ""

	def get_broker(self):
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST,1)
			sock.sendto("BDP01REQ", ("255.255.255.255",5555))

			sock.settimeout(1.0)
			data,address = sock.recvfrom(1024)
			if data == "BDP01REP":
				self.broker = "tcp://"+ address[0] + ":5555"
		except:
			self.broker = None

	def reconnect_to_broker(self):
		"""
		Connect or reconnect to broker
		"""
		if self.client:
			self.poller.unregister(self.client)
			self.client.close()
			
		self.client = self.ctx.socket(zmq.DEALER)
		self.client.linger = 0
		self.client.connect(self.broker)
		self.poller.register(self.client, zmq.POLLIN)
		if self.verbose:
			logging.info("I: connecting to broker at %s...", self.broker)

	def send(self, service, request):
		"""
		Send request to broker
		"""
		if not isinstance(request, list):
			request = [request]

		# Prefix request with protocol frames
		# Frame 0: empty (REQ emulation)
		# Frame 1: "MDPCxy" (six bytes, MDP/Client x.y)
		# Frame 2: Service name (printable string)

		request = ['', MDP.C_CLIENT, service] + request
		if self.verbose:
			logging.warn("I: send request to '%s' service: ", service)
			dump(request)
		self.client.send_multipart(request)

	def recv(self):
		"""
		Returns the reply message or None if there was no reply.
		"""
		try:
			items = self.poller.poll(self.timeout)
		except KeyboardInterrupt:
			return # interrupted

		if items:
			# if we got a reply, process it
			msg = self.client.recv_multipart()
			if self.verbose:
				logging.info("I: received reply:")
				dump(msg)

			# Don't try to handle errors, just assert noisily
			assert len(msg) >= 4

			empty = msg.pop(0)
			header = msg.pop(0)
			assert MDP.C_CLIENT == header

			service = msg.pop(0)
			return msg
		else:      
			logging.warn("W: permanent error, abandoning request")
