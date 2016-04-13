#!/usr/bin/env python

import os
import sys
fileDir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(fileDir, "../../bolt"))
import MDP

from client_api import MajordomoClient

class EchoClient(MajordomoClient):

	def __init__(self,verbose=False):
		super(EchoClient,self).__init__(verbose)

	def run_task(self):
		requests = 10
		count = 0
    
		for i in xrange(requests):
			request = "Hello world %d" % i
			try:
				self.send("echo", request)
				reply = self.recv()
				if reply is None:
					break
				else:
					print reply

				count = i
			except KeyboardInterrupt:
				print "send interrupted, aborting"
				return

		print "%i requests/replies processed" % (count+1)


def test():
	bolt_client = EchoClient()
	bolt_client.run_task()
	
if __name__ == "__main__":
	test()
