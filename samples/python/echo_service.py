#!/usr/bin/env python

import os
import sys
fileDir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(fileDir, "../../bolt"))
import MDP

from worker_api import MajordomoWorker

'''

'''
class EchoService(MajordomoWorker):

	def __init__(self, service_name, broker="tcp://localhost:5555",verbose=False):
		
		super(EchoService,self).__init__(broker,service_name,verbose)
			
	def client_request_handler(self, request):
		'''
		this method should be derived by child class.
		'''
		#msg = "%s returned." % request.pop(0)
		msg = request.pop(0)
		
		return [msg]
	


def test():
	bolt_worker = EchoService("echo")
	bolt_worker.serve_forever()
	bolt_worker.destroy()
	
	
if __name__ == "__main__":
	test()
