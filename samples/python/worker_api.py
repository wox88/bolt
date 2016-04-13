#!/usr/bin/env python

'''
 Majordomo Protocol Worker API, Python version
 Implements the MDP/Worker spec at http:#rfc.zeromq.org/spec:7.
 
 Modified from python examples by Min RK, <benjaminrk@gmail.com>
'''

import zmq
import time
import logging

import MDP						# MajorDomo protocol constants
from zhelpers import dump

class MajordomoWorker(object):
	'''
	Implements the MDP/Worker.
	
	A worker should register to the broker first, then start a loop to [receive a request from broker and send a reply].
	'''
	
	HEARTBEAT_LIVENESS = 3 	# 3-5 is reasonable
	broker = None
	ctx = None
	service = None

	wsocket = None 			# Socket to broker
	heartbeat_at = 0 			# When to send HEARTBEAT (relative to time.time(), so in seconds)
	liveness = 0 				# How many attempts left
	heartbeat = 2500 			# Heartbeat delay, msecs
	reconnect = 2500 			# Reconnect delay, msecs

	# Internal state
	timeout = 2500 			# poller timeout
	verbose = False 			# Print activity to stdout
	reply_to = None			# Return address, if any
	
	
	def __init__(self, broker, service, verbose=False):
		self.broker = broker
		self.service = service
		self.verbose = verbose
		self.ctx = zmq.Context()
		self.poller = zmq.Poller()
		self.reconnect_to_broker()
		logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",level=logging.INFO)
		
	def reconnect_to_broker(self):
		"""
		Connect or reconnect to broker
		"""
		
		# Reset the socket to the broker.
		if self.wsocket:
			self.poller.unregister(self.wsocket)
			self.wsocket.close()
			
		self.wsocket = self.ctx.socket(zmq.DEALER)
		self.wsocket.linger = 0
		self.wsocket.connect(self.broker)
		self.poller.register(self.wsocket, zmq.POLLIN)

		# Register service with broker
		self.register_service(self.service)

		# Reset liveness & heartbeat.
		# If liveness hits zero, queue is considered disconnected
		self.liveness = self.HEARTBEAT_LIVENESS
		self.heartbeat_at = time.time() + 1e-3 * self.heartbeat
		
	def register_service(self, service_name):
		"""
		command : READY
		msg : 
			Frame 3 - name of this service.
		"""
		command = MDP.W_READY
		msg = [service_name]
		self.send_to_broker(command, msg)
		
	def send_reply(self, reply_to, reply):
		"""
		command : REPLY
		msg : 
			Frame 3 - the target of this reply.
			Frame 4 - empty
			Frame 5 - the content of this reply.
		"""
		command = MDP.W_REPLY
		
		if not isinstance(reply, list):
			reply = [reply]
		
		msg = [reply_to, '', ] + reply
		self.send_to_broker(command, msg)
		
	def send_heartbeat(self):
		"""
		command : HEARTBEAT
		msg : None
		"""
		command = MDP.W_HEARTBEAT
		msg = []
		self.send_to_broker(command, msg)
		
	def send_disconnect(self):
		"""
		command : DISCONNECT
		msg : None
		"""
		command = MDP.W_DISCONNECT
		msg = []
		self.send_to_broker(command, msg)
		
	def send_to_broker(self, command, msg):
		"""
		Send a message to the broker.

		The frame format of a message:
			Frame 0: empty
			Frame 1: version of mdp.
			
			Frame 2: command, such as
				data		command			direction	
				0x01		READY				(out)
				0x02		REQUEST			(in)
				0x03		REPLY				(out)
				0x04		HEARTBEAT		(both)
				0x05		DISCONNECT		(both)
			Frame 3+: msg
		 """
		 
		if not isinstance(msg, list):
			msg = [msg]

		# the data that broker received consist of a frame before msg.
		msg = ['', MDP.W_WORKER, command] + msg
		
		#if self.verbose:
		#	logging.info("I: sending %s to broker", command)
		#	dump(msg)
		
		# finally, send the msg.	
		#print msg
		self.wsocket.send_multipart(msg)
		
	def client_request_handler(self, request):
		"""
		This method should be overrided by the child class.
		"""
		pass
		
	def msg_handler(self,msg):
		"""
		May receive 3 kinds of msg:
			REQUEST
			HEARTBEAT
			DISCONNECT
		"""
		#print msg
		
		if len(msg) < 3 or msg.pop(0) != '' or msg.pop(0) != MDP.W_WORKER :
			return # error msg
	
		command = msg.pop(0)
		if command == MDP.W_REQUEST:
			# We should pop and save as many addresses as there are
			# up to a null part, but for now, just save one...
			self.reply_to = msg.pop(0)
			
			if self.reply_to == None or msg.pop(0) != '':
				return # error msg
		
			# handle a valid request, then send to the target.
			reply = self.client_request_handler(msg)
			self.send_reply(self.reply_to, reply)
		elif command == MDP.W_HEARTBEAT:
			# Do nothing for heartbeats
			pass
		elif command == MDP.W_DISCONNECT:
			self.reconnect_to_broker()
		else :
			#logging.error("E: invalid input message: ")
			#dump(msg)
			pass
			
	def serve_forever(self):
		"""
		Try to get a request from the broker, then handle the msg received.
		"""
		logging.info("Service '%s' Registered.",self.service)
		
		while True:
			# Poll socket for a reply, with timeout
			try:
				items = self.poller.poll(self.timeout)
			except KeyboardInterrupt:
				break # Interrupted
	
			if items: # received a msg.
				msg = self.wsocket.recv_multipart()
				#if self.verbose:
				#	logging.info("I: received message from broker: ")
				#	dump(msg)
				
				# handle the received msg.
				self.msg_handler(msg)
				self.liveness = self.HEARTBEAT_LIVENESS
			else: # no msg or heartbeat received.
				self.liveness -= 1
				if self.liveness == 0:
					#if self.verbose:
					#	logging.warn("W: disconnected from broker - retrying...")
						
					try:
						time.sleep(1e-3*self.reconnect)
					except KeyboardInterrupt:
						break
					
					# try to reconnect	
					self.reconnect_to_broker()
			
			# Send HEARTBEAT if it's time
			if time.time() > self.heartbeat_at:
				self.send_heartbeat()
				self.heartbeat_at = time.time() + 1e-3*self.heartbeat
					
		#logging.warn("W: interrupt received, killing worker...")
		return None
			
	def destroy(self):
		# context.destroy depends on pyzmq >= 2.1.10
		self.ctx.destroy(0)
	
	
