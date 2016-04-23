#!/usr/bin/env python

import os
import sys
fileDir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(fileDir, "../../bolt"))
import MDP

from worker_api import MajordomoWorker

import time
import numpy
import pickle
import StringIO
from PIL import Image

# openface settings
import openface
align = openface.AlignDlib("/usr/local/lib/python2.7/dist-packages/openface/models/dlib/shape_predictor_68_face_landmarks.dat")
net = openface.TorchNeuralNet("/usr/local/lib/python2.7/dist-packages/openface/models/openface/nn4.small2.v1.t7", imgDim=96,cuda=False)

def getRep(data):
	if data is not None:
		imgF = StringIO.StringIO()
		imgF.write(data)
		imgF.seek(0)
		img = Image.open(imgF)

		buf = numpy.fliplr(numpy.asarray(img))
		rgbFrame = numpy.zeros((240, 320, 3), dtype=numpy.uint8)
		rgbFrame[:, :, 0] = buf[:, :, 2]
		rgbFrame[:, :, 1] = buf[:, :, 1]
		rgbFrame[:, :, 2] = buf[:, :, 0]

		#		
		bb = align.getLargestFaceBoundingBox(rgbFrame)
		alignedFace = align.align(96, rgbFrame, bb,landmarkIndices=openface.AlignDlib.OUTER_EYES_AND_NOSE)

		if alignedFace is not None:
			rep = net.forward(alignedFace)
			return (rep,bb)

	return (None,None)

def recognize(img):
	with open("/usr/local/lib/python2.7/dist-packages/openface/models/ronis_classifier.pkl",'r') as f:
		(le, clf) = pickle.load(f)
		tmp,bb = getRep(img)
		if tmp is not None:
			rep = tmp.reshape(1, -1)
		else:
			return (None,None)	

		predictions = clf.predict_proba(rep).ravel()
		maxI = numpy.argmax(predictions)
		person = le.inverse_transform(maxI)
		confidence = predictions[maxI]
		
		return (person,bb)

'''

'''
class OpenfaceService(MajordomoWorker):

	def __init__(self, service_name, broker="tcp://localhost:5555",verbose=False):
		
		super(OpenfaceService,self).__init__(broker,service_name,verbose)
			
	def client_request_handler(self, request):
		'''
		this method should be derived by child class.
		'''
		#msg = "%s returned." % request.pop(0)
		msg = request.pop(0)
		
		#f=open("./imgs/out"+ str(time.time())+".jpg",'w')
		#f.write(msg)
		#f.close()

		start = time.time()
		person,bb = recognize(msg)

		if person is not None:
			ret = person
		else:
			ret = "unknown"

		print ret
		print("took {} seconds.".format(time.time()-start))
		return [ret]
	


def test():
	bolt_worker = OpenfaceService("openface")
	bolt_worker.serve_forever()
	bolt_worker.destroy()
	
	
if __name__ == "__main__":
	test()
