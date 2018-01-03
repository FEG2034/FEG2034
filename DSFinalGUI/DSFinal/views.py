# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.template import loader
from django.http import HttpResponse
from django.http import JsonResponse

import os
import time

# Create your views here.
def index(request):
	template = loader.get_template( 'DSFinal/index.html' )
	context = {}
	return HttpResponse(template.render(context, request))

def RunDataAnalysis(request):
	params = request.POST#.getlist("queryParams")
	startDate = params["startDate"]
	endDate = params["endDate"]
	eventType = params["eventType"]
	outputFileName = str(time.time())+".output"
	
	os.system("spark-submit GDELT_refactored.py " + startDate + " " + endDate + " " + eventType + " > " + outputFileName)
	sparkResult = None
	with open(outputFileName) as outputFile:
		sparkResult = outputFile.readlines()
	print sparkResult
	return JsonResponse({"SparkResult":sparkResult})
