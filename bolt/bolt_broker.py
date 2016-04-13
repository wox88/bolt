#!/usr/bin/env python

"""
 Majordomo Protocol broker
 A minimal implementation of http:#rfc.zeromq.org/spec:7 and spec:8

 Modified from python examples by Min RK, <benjaminrk@gmail.com>
"""

import logging
import sys
import time
import zmq
import binascii
import SocketServer

import MDP
from zhelpers import dump
from bolt_discovery import UDPReceivedHandler

class Service(object):
	"""
	a single Service
	"""
	name = None 			# Service name
	requests = None 		# List of client requests
	waiting = None 		# List of waiting workers's id

	def __init__(self, name):
		self.name = name
		self.requests = []
		self.waiting = []


class Worker(object):
	"""
	a Worker, idle or active
	"""
	worker_id = None 					# hex Identity of worker, can be used as address
	service = None 					# Owning service, if known
	expiry = None 						# expires at this point, unless heartbeat

	def __init__(self, worker_id, lifetime):
		self.worker_id = worker_id
		self.expiry = time.time() + 1e-3*lifetime


class MajordomoBroker(object):
	"""
	Broker of Majordomo Protocol. 
	"""

	# We'd normally pull these from config data
	INTERNAL_SERVICE_PREFIX = "mmi."
	HEARTBEAT_LIVENESS = 3 				# 3-5 is reasonable
	HEARTBEAT_INTERVAL = 2500 			# msecs
	HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

	ctx = None 								# Our context
	socket = None 							# Socket for clients & workers
	poller = None 							# our Poller

	heartbeat_at = None					# When to send HEARTBEAT
	services = None 						# known services
	workers = None 						# known workers
	waiting = None 						# idle workers

	verbose = False 						# Print activity to stdout

	
	def __init__(self, verbose=False):
		"""
		Initialize broker state.
		"""
		self.verbose = verbose
		self.services = {}
		self.workers = {}
		self.waiting = []
		self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL
		self.ctx = zmq.Context()
		self.socket = self.ctx.socket(zmq.ROUTER)
		self.socket.linger = 0
		self.poller = zmq.Poller()
		self.poller.register(self.socket, zmq.POLLIN)
		logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",level=logging.INFO)
		self.bind("tcp://*:5555")

	def serve_forever(self):
		"""
		Main broker work happens here
		Wait for the msgs from client & worker, then process them.
		"""
		logging.info("Bolt Broker started.")

		while True:
			try:
				items = self.poller.poll(self.HEARTBEAT_INTERVAL)
			except KeyboardInterrupt:
				break # Interrupted
				
			# received a msg.
			if items:
				msg = self.socket.recv_multipart()
				#if self.verbose:
				#	logging.info("I: received message:")
				#	dump(msg)

				# handle the received msg.
				#print msg
				self.msg_handler(msg)
			
			self.purge_workers()
			self.send_heartbeats()
      	#self.purge_workers()
      	#self.send_heartbeats()    

	def msg_handler(self,msg):
		#print msg
		sender = msg.pop(0)	# where this msg from.
		sender_id = binascii.hexlify(sender) # get the string of hex.
		empty = msg.pop(0)
		if empty != '':
			return # error msg
      	
		header = msg.pop(0)
		if (MDP.C_CLIENT == header): 
			# msg from a client
			self.process_client(sender_id, msg)
		elif (MDP.W_WORKER == header):
			# msg from a worker
			self.process_worker(sender_id, msg)
		else:
			#logging.error("E: invalid message:")
			#dump(msg)
			pass # error msg
			
	def process_client(self, client_id, msg):
		"""
		Just 1 kind of msg:
			REQUEST
			
		Process a request coming from a client.
		"""
		assert len(msg) >= 2 # Service name + body
		service = msg.pop(0)
		
		client_address = binascii.unhexlify(client_id)
		msg = [client_address,''] + msg  # Set reply return address to client sender
		
		if service.startswith(self.INTERNAL_SERVICE_PREFIX):
			self.service_internal(service, msg)
		else:
			self.dispatch(self.get_service(service), msg)
			
	def service_internal(self, service, msg):
		"""
		Handle internal service according to 8/MMI specification
		"""
		returncode = "501"
		if "mmi.service" == service:
			name = msg[-1]
			returncode = "200" if name in self.services else "404"

		msg[-1] = returncode

		# insert the protocol header and service name after the routing envelope ([client, ''])
		msg = msg[:2] + [MDP.C_CLIENT, service] + msg[2:]
		self.socket.send_multipart(msg)
		
	def dispatch(self, service, msg):
		"""
		Dispatch requests to waiting workers as possible
		"""
		assert (service is not None)
		
		if msg is not None:# Queue message if any
			service.requests.append(msg)
		
		self.purge_workers()
		
		while service.waiting and service.requests:
			msg = service.requests.pop(0)
			worker_id = service.waiting.pop(0)
			#worker = self.get_worker(worker_id)
			self.waiting.remove(worker_id)
			self.send_to_worker(worker_id, MDP.W_REQUEST, msg)
			
	def send_to_worker(self, worker_id, command, msg=None):
		"""
		Send message to worker.

		If message is provided, sends that message.
		 """

		if msg is None:
			msg = []
		elif not isinstance(msg, list):
			msg = [msg]

		# Stack routing and protocol envelopes to start of message
		# and routing envelope
		address = binascii.unhexlify(worker_id)
		msg = [address, '', MDP.W_WORKER, command] + msg

		#if self.verbose:
		#	logging.info("I: sending %r to worker", command)
		#	dump(msg)

		self.socket.send_multipart(msg)
    
	def process_worker(self, worker_id, msg):
		"""
		Process message sent by a worker.
		May receive 4 kinds of msg:
			READY
			REPLY
			HEARTBEAT
			DISCONNECT
		Args:
			worker_id - the id of the worker
			msg - the msg.
		"""
		assert len(msg) >= 1 # At least, command

		command = msg.pop(0)

		is_worker_existed = worker_id in self.workers
		worker = self.get_worker(worker_id)
		
		if (MDP.W_READY == command): # Ready
			# register a service
			assert len(msg) >= 1 # At least, a service name
			service = msg.pop(0)
			
			if (is_worker_existed or service.startswith(self.INTERNAL_SERVICE_PREFIX)):
				# error msg
				# register for more than once, or register the reserved service.
				self.delete_worker(worker_id, True)
			else:
				# Attach worker to service and mark as idle
				worker.service = self.get_service(service)
				self.add_worker_to_waiting_list(worker_id)	
		elif (MDP.W_REPLY == command): # Reply		
			if (is_worker_existed):
				# Remove & save client return envelope and insert the
				# protocol header and service name, then rewrap envelope.
				self.send_reply_to_client(worker.service.name,msg)
				self.add_worker_to_waiting_list(worker_id)
			else:
				self.delete_worker(worker_id, True)
		elif (MDP.W_HEARTBEAT == command): # Heartbeat
			if (is_worker_existed):
				worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
			else:
				self.delete_worker(worker_id, True)
		elif (MDP.W_DISCONNECT == command): # Disconnect
			self.delete_worker(worker_id, False)
		else: # error msg
			#logging.error("E: invalid message:")
			#dump(msg)
			pass
			
	def delete_worker(self, worker_id, disconnect):
		"""
		Deletes worker from all data structures, and deletes worker.
		"""
		worker = self.get_worker(worker_id)
		assert worker is not None
		
		if disconnect:
			self.send_to_worker(worker_id, MDP.W_DISCONNECT, None)

		if worker.service is not None:
			worker.service.waiting.remove(worker_id)
			
		self.workers.pop(worker_id)

	def get_worker(self, worker_id):
		"""
		Finds the worker (creates if necessary).
		"""
		assert (worker_id is not None)
		
		worker = self.workers.get(worker_id)
		if (worker is None): # this worker registers for first time 
			worker = Worker(worker_id, self.HEARTBEAT_EXPIRY)
			self.workers[worker_id] = worker
			#if self.verbose:
			#	logging.info("I: registering new worker: %s", identity)

		return worker    

	def get_service(self, service_name):
		"""
		Locates the service (creates if necessary).
		"""
		assert (service_name is not None)
		
		service = self.services.get(service_name)
		if (service is None):
			service = Service(service_name)
			self.services[service_name] = service

		return service

	def add_worker_to_waiting_list(self, worker_id):
		"""
		This worker is now waiting for work.
		"""
		# Queue to broker and service waiting lists
 		self.waiting.append(worker_id)
 		worker = self.get_worker(worker_id)
		worker.service.waiting.append(worker_id)
		worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
		self.dispatch(worker.service, None)

	def send_reply_to_client(self, service_name, msg):
		client = msg.pop(0)
		empty = msg.pop(0)
		if empty != '':
			return # error msg.
				
		msg = [client, '', MDP.C_CLIENT, service_name] + msg
		
		#print msg
		self.socket.send_multipart(msg)


	def purge_workers(self):
		"""
		Look for & kill expired workers.

		Workers are oldest to most recent, so we stop at the first alive worker.
		 """
		while self.waiting:	
			worker_id = self.waiting[0]
			worker = self.get_worker(worker_id)
			if worker.expiry < time.time():
				logging.info("I: deleting expired worker: %s", worker_id)
				self.delete_worker(worker_id,False)
				self.waiting.pop(0)
			else:
				break
		
	def send_heartbeats(self):
		"""
		Send heartbeats to idle workers if it's time
		"""
		if (time.time() > self.heartbeat_at):
			for worker_id in self.waiting:
				self.send_to_worker(worker_id, MDP.W_HEARTBEAT, None)
				
			self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL
			
	def bind(self, endpoint):
		"""
		Bind broker to endpoint, can call this multiple times.

		We use a single socket for both clients and workers.
		"""
		self.socket.bind(endpoint)
		#logging.info("I: MDP broker/0.1.1 is active at %s", endpoint)
			
	def destroy(self):
		"""
		Disconnect all workers, destroy context.
		"""
		while self.workers:
			self.delete_worker(self.workers.values()[0], True)
			self.ctx.destroy(0)

def run():
    """create and start new broker"""
    broker = MajordomoBroker()
    broker.serve_forever()

if __name__ == '__main__':
    run()



