from django.shortcuts import render,HttpResponse
import pymysql
from Util.DataProc.data_acquire import get_data, get_dates,get_state_heatmap,get_date_lowlevel
import json
# conn = pymysql.connect(
#     host='127.0.0.1',
#     user='root', password='LRc19980307',
#     database='PepperDatabase',
#     autocommit=True,
#     charset='utf8')


def index(request):
    return render(request, 'index.html')


def confirmed(request):
    return render(request, 'confirmed_cases.html')

def county_confirmed(request):
    return render(request, 'confirmed_cases_county.html')


def labeled(request):
    return render(request, 'labeled_confirmed.html')


def high_level(request):
    return render(request, 'predict_citymap.html')


def low_level(request):
    return render(request, 'predict_heatmap.html')


def dataprocess(request):
    print("request", request.GET.get("state"))
    if request.GET.get("command") == "start_up":
        return HttpResponse(json.dumps(get_dates(request.GET.get("map_type"))))
    if request.GET.get("command") == "start_up_heatmap":
        return HttpResponse(json.dumps(get_state_heatmap()))
    if request.GET.get("command") == "start_up_lowlevel":
        return HttpResponse(json.dumps(get_date_lowlevel()))
    if request.GET.get("command") == "start_up":
        return HttpResponse(json.dumps(get_dates(request.GET.get("map_type"))))
    if request.GET.get("command") == "get_confirmed_metro":
        return HttpResponse(json.dumps(get_data(request.GET.get("date"), "confirmed", request.GET.get("state"))))
    if request.GET.get("command") == "get_confirmed_county":
        return HttpResponse(json.dumps(get_data(request.GET.get("date"), "confirmed_county", request.GET.get("state"))))
    if request.GET.get("command") == "get_labeled_metro":
        return HttpResponse(json.dumps(get_data(request.GET.get("date"), "labeled", request.GET.get("state"))))
    if request.GET.get("command") == "get_predicted_metro":
        return HttpResponse(json.dumps(get_data(request.GET.get("date"), "predicted", request.GET.get("state"))))
    if request.GET.get("command") == "get_heatmap_points":
        return HttpResponse(json.dumps(get_data(request.GET.get("date"), "heatmap", request.GET.get("state"))))