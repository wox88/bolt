#!/usr/bin/env python

import re
import time
import threading
import Queue

from os import sep
from os import curdir

from BaseHTTPServer import BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from BaseHTTPServer import HTTPServer

import os
import sys
fileDir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(fileDir, "../../bolt"))
import MDP

from worker_api import MajordomoWorker

share_queue = Queue.Queue()

'''

'''
class EchoService(MajordomoWorker):

	def __init__(self, service_name, broker="tcp://localhost:5555",verbose=False):
		super(EchoService,self).__init__(broker,service_name,verbose)

	def client_request_handler(self, request):
		global share_queue
		'''
		this method should be derived by child class.
		'''
		#msg = "%s returned." % request.pop(0)
		msg = request.pop(0)
		share_queue.put_nowait(msg)

		#print msg
		#f = open('test2.jpg','w')
		#f.write(msg)
		#f.close

		return ["test"]



class MJPEGStreamHandler(BaseHTTPRequestHandler, object):
	def do_GET(self):
		global share_queue
		self.data_queue = share_queue

		try:
			self.path = re.sub('[^.a-zA-Z0-9]', "", str(self.path))
			if self.path== "" or self.path is None or self.path[:1] == ".":
				return

			if self.path.endswith(".html"):
				f = open(curdir + sep + self.path)
				self.send_response(200)
				self.send_header('Content-type',	'text/html')
				self.end_headers()
				self.wfile.write(f.read())
				f.close()
				return

			if self.path.endswith(".mjpeg"):
				self.send_response(200)
				self.wfile.write("Content-Type: multipart/x-mixed-replace; boundary=--aaboundary")
				self.wfile.write("\r\n\r\n")

				while 1:
					if self.data_queue.empty() == False:
						image_data = self.data_queue.get()
						self.wfile.write("--aaboundary\r\n")
						self.wfile.write("Content-Type: image/jpeg\r\n")
						self.wfile.write("Content-length: " + str(len(image_data)) + "\r\n\r\n")
						self.wfile.write(image_data)
						self.wfile.write("\r\n\r\n\r\n")
						time.sleep(0.001)

				return

			if self.path.endswith(".jpeg"):
				f = open(curdir + sep + self.path)
				self.send_response(200)
				self.send_header('Content-type', 'image/jpeg')
				self.end_headers()
				self.wfile.write(f.read())
				f.close()
				return

			return
		except IOError:
			self.send_error(404,'File Not Found: %s' % self.path)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
	stopped = False
    
	"""Handle requests in a separate thread."""
	def serve_forever(self):
		while not self.stopped:
			self.handle_request()

	def terminate(self):
		self.server_close()
		self.stopped = True

		# close all thread
		if self.socket != -1:
			self.socket.close()

if __name__ == "__main__":

	http_server = ThreadedHTTPServer(('0.0.0.0', 8080), MJPEGStreamHandler)
	http_server_thread = threading.Thread(target=http_server.serve_forever)
	http_server_thread.daemon = True
	http_server_thread.start()

	bolt_worker = EchoService("image")
	bolt_worker.serve_forever()
	
	while True:
		time.sleep(1)

	bolt_worker.destroy()

