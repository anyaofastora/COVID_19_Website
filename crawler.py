import requests
import pandas as pd
import os
import json
import pymysql
import numpy as np
conn = pymysql.connect(
    host='localhost',
    user='root', password='LRc19980307',
    database='us_metro',
    charset='utf8')
cursor = conn.cursor()
from bs4 import BeautifulSoup

def getConfirmedFile():
    url = "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/5B8YM8"
    html_str = requests.get(url)


def get_file_list(file_path):
    """
    获取文件夹下所有的文件名称
    :param file_path:
    :return:
    """
    dir_list = os.listdir(file_path)
    if not dir_list:
        return
    else:
        dir_list = sorted(dir_list,key=lambda x: os.path.getctime(os.path.join(file_path, x)))
        # print(dir_list)
        return dir_list


def dumpjson(data,metro,date):
    """
    data:data list
    metro:city name + state name list
    date:date
    """
    confirmed = {}
    confirmed["date"] = date
    data_list = []
    for i in data:
        print(i)



def dataConverter():
    df = pd.read_csv("cbsas.timeseries.csv")
    filelist = get_file_list("./Data/confirmed")
    # datelist = [i for i in df][12:]
    # print(datelist)
    fips_list = df["fips"].tolist()
    date_list = df["date"].tolist()
    case_list = df["actuals.cases"].tolist()
    print(case_list[:5], pd.isnull(case_list[0]))
    city_list = []
    state_list = []
    lines = {}
    for i in range(len(fips_list)):
        sql = "select city,state from metro_fips where fips =%s"%(str(fips_list[i]))
        cursor.execute(sql)
        result = cursor.fetchall()[0]
        city_list.append(result[0])
        state_list.append(result[1])
    print(len(fips_list),len(city_list),len(date_list))
    for j in range(len(city_list)):
        if case_list[j] != None:
            lines[city_list[j]] = case_list[j]
        else:
            lines[city_list[j]] = 0
    data_dict = {}
    for i in range(len(city_list)):
        data_dict[date_list[i]] = {}
    for i in range(len(city_list)):
        if pd.isnull(case_list[i]):
            data_dict[date_list[i]][str(fips_list[i])] = 0
        else:
            data_dict[date_list[i]][str(fips_list[i])] = case_list[i]
    for i in range(len(city_list)):
        with open("./Data/confirmed/%s.json" % (date_list[i]), 'w+', encoding='utf-8') as json_file:
            json.dump(data_dict[date_list[i]], json_file, ensure_ascii=False)


    datalist = []
    #print(citylist)
    #print(datalist)
    # for i in datelist:
    #     if i+".json" not in filelist:
    #         #print(i)
    #         data = df[i].tolist()
    #         lines = {}
    #         for j in range(len(citylist)):
    #             lines[citylist[j]] = data[j]
    #         i = i.replace("/", "-")
    #         with open("./Data/confirmed/%s.json"%(i), 'w+', encoding='utf-8') as json_file:
    #             print(i)
    #             json.dump(lines, json_file, ensure_ascii=False)

def convert_date(date):
    list = date.split("-")



if __name__ == '__main__':
    dataConverter()

