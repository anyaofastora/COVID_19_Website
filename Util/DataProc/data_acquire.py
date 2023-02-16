import json
import pymysql
import time
import os
import numpy as np
import pandas as pd
import pickle

# MYSQL_HOST = "127.0.0.1"
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "LRc19980307"
#
MYSQL_HOST = "localhost"
MYSQL_USER = "covid19"
MYSQL_PASSWORD = "LRc19980307_"


def read_state_coord():
    state = pd.read_csv("statelatlong.csv")
    state = state.values
    state_coord = {}
    for i in range(np.shape(state)[0]):
        state_coord[state[i][0]] = [state[i][1],state[i][2]]
    state_coord["all states"] = state_coord["OH"]
    return state_coord


STATE_COORD = read_state_coord()


def get_dates(maptype):
    #dir_list = os.listdir("./Data/"+maptype)
    if maptype == "metro":
        dir_list = os.listdir("./Data/confirmed")
    if maptype == "county":
        dir_list = os.listdir("./Data/county_confirmed")
    for i in range(len(dir_list)):
        dir_list[i] = dir_list[i].strip(".json")
        #print(dir_list[i])
        if dir_list[i][5] == "0":
            dir_list[i] = dir_list[i][0:5] + dir_list[i][6:]
            if dir_list[i][7] == "0":
                dir_list[i] = dir_list[i][0:7] + dir_list[i][8:]
        else:
            if dir_list[i][8] == "0":
                dir_list[i] = dir_list[i][0:8] + dir_list[i][9:]
        # print(dir_list[i])
    result = {"status": "success", "count": len(dir_list), "availableDate": dir_list}
    if not dir_list:
        return "Fail"
    else:
        return result

def get_state_heatmap():
    dir_list = os.listdir("./Data/heatmap")
    state_list = []
    for i in range(len(dir_list)):
        #print(dir_list[i])
        state_list.append(dir_list[i][0:2])
        dir_list[i] = dir_list[i][3:]
        #print(dir_list[i])
        if dir_list[i][5] == "0":
            dir_list[i] = dir_list[i][0:5] + dir_list[i][6:]
            if dir_list[i][7] == "0":
                dir_list[i] = dir_list[i][0:7] + dir_list[i][8:]
        else:
            if dir_list[i][8] == "0":
                dir_list[i] = dir_list[i][0:8] + dir_list[i][9:]

    #print("state_list",state_list)
    state_list = list(set(state_list))
    result = {"status": "success", "count": len(state_list), "availableDate": dir_list, "availableState": state_list}
    return result


def get_date_lowlevel():
    dir_list = os.listdir("./Data/predicted")
    for i in range(len(dir_list)):
        dir_list[i] = dir_list[i].strip(".json")
        if dir_list[i][5] == "0":
            dir_list[i] = dir_list[i][0:5] + dir_list[i][6:]
            if dir_list[i][7] == "0":
                dir_list[i] = dir_list[i][0:7] + dir_list[i][8:]
        else:
            if dir_list[i][8] == "0":
                dir_list[i] = dir_list[i][0:8] + dir_list[i][9:]
    result = {"status": "success", "count": len(dir_list), "availableDate": dir_list}
    return result


def get_data_multi(cities, states, cursor):
    for city in cities:
        for state in states:
            sql = 'select latitude,longtitude from metro where city="%s" and state_id="%s";' % (city, state)
            cursor.execute(sql)
            result = cursor.fetchall()
            # print(result)
            if result != ():
                # print(sql)
                lat = result[0][0]
                lon = result[0][1]
                # latlist.append(result[0][0])
                # lonlist.append(result[0][1])
                return lat, lon
    return 0, 0


def get_confirmed_county_data(date, selected_state):
    """
       date:date
       map:maptype
       selected_state:selected_state to display
       return: a dict with status, count, metro list, latitude list, longtitude list,
       """
    # print(selected_state)
    if date is None:
        return {"status": "unsuccess"}
    else:
        if selected_state not in ["all states", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
                                  "IL", "IN", "IA", "KS", "KY", "LA",
                                  "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC",
                                  "ND", "OH", "OK",
                                  "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"]:
            selected_state = "all states"
        # print(selected_state)
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database='us_metro',
            autocommit=True,
            charset='utf8')
        cursor = conn.cursor()
        time1 = time.time()
        with open("./Data/county_confirmed/%s.json" % date, 'r', encoding='utf-8') as json_file:
            model = json.load(json_file)
        time2 = time.time()
        result_list = []
        single_list = []
        count = 0
        fips_list = list(model.keys())
        sql = 'select lat,lon,county,state,fips from county_fips where'
        if selected_state == "all states":
            for i in range(len(fips_list)-1):
                sql += ' (fips = %s ) or' % (fips_list[i])
            sql += ' (fips = %s)' % (fips_list[len(fips_list)-1])

        else:
            for i in range(len(fips_list)-1):
                sql += ' (fips = %s and state like "%%%s%%") or' % (fips_list[i],selected_state)
            sql += ' (fips = %s and state like "%%%s%%")' % (fips_list[len(fips_list)-1],selected_state)
        #single_list[len(single_list) - 1][0], single_list[len(single_list) - 1][1])
        #print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        #print(result)
        # for i in range(len(result)):
        #     print(i,result[i])
        for i in range(len(result)):
            # print(result[i])
            # print(model)
            # print(model[result[i][4]])

            result_list.append(
                {'city': result[i][2] + " " + result[i][3], "latitude": result[i][0], "longitude": result[i][1],
                 "data-total": model[str(result[i][4])][0],
                 "data-new": model[str(result[i][4])][1]})
            count += 1

        time3 = time.time()
        print(time2 - time1, time3 - time2)
        conn.close()
        result = {"status": "success", "count": count, "data_list": result_list, "state_coord": STATE_COORD[selected_state]}
        # print(len(metro_list), len(result_lat_list), len(result_lon_list), len(result_data))
        # for i in range(len(metro_list)):
        #     print(i,metro_list[i])
        # print(result)
        return result


def get_confirmed_data(date, selected_state):
    """
       date:date
       map:maptype
       selected_state:selected_state to display
       return: a dict with status, count, metro list, latitude list, longtitude list,
       """
    # print(selected_state)
    if date is None:
        return {"status": "unsuccess"}
    else:
        if selected_state not in ["all states", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
                                  "IL", "IN", "IA", "KS", "KY", "LA",
                                  "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC",
                                  "ND", "OH", "OK",
                                  "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"]:
            selected_state = "all states"
        # print(selected_state)
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database='us_metro',
            autocommit=True,
            charset='utf8')
        cursor = conn.cursor()
        time1 = time.time()
        with open("./Data/confirmed/%s.json" % date, 'r', encoding='utf-8') as json_file:
            model = json.load(json_file)
        time2 = time.time()
        result_list = []
        single_list = []
        count = 0
        fips_list = list(model.keys())
        # for i in model:
        #     # states = i[-len(i) + i.rfind(" ") + 1:].split("-")
        #     # cities = i[:-len(i) + i.rfind(" "):1].split("-")
        #     # print(i,len(i)-i.rfind(" "),cities,states,)
        #     # print(i, len(i) - i.rfind(" "), cities, states, selected_state)
        #     #single_list.append([cities[0], states[0]])
        #
        #         # for multiple county in one row
        #         else:
        #             latlist = []
        #             lonlist = []
        #             lat, lon = get_data_multi(cities, states, cursor)
        #             if lat != 0 and lon != 0:
        #                 # latitude = sum(latlist)/len(latlist)
        #                 # longtitue = sum(lonlist)/len(lonlist)
        #                 # print(latitude,longtitue)
        #                 # resultlist[str(count)]=[i,latitude,longtitue,model[i]]
        #                 result_list.append({'city': i, "latitude": lat, "longitude": lon, "data": model[i]})
        #                 count += 1
        #             else:
        #                 print(i, cities, states)

        # for single county (this increases speed of searching)
        sql = 'select latitude,longitude,city,state,fips from metro_fips where'
        if selected_state == "all states":
            for i in range(len(fips_list)-1):
                sql += ' (fips = %s ) or' % (fips_list[i])
            sql += ' (fips = %s)' % (fips_list[len(fips_list)-1])

        else:
            for i in range(len(fips_list)-1):
                sql += ' (fips = %s and state like "%%%s%%") or' % (fips_list[i],selected_state)
            sql += ' (fips = %s and state like "%%%s%%")' % (fips_list[len(fips_list)-1],selected_state)
        #single_list[len(single_list) - 1][0], single_list[len(single_list) - 1][1])
        #print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        #print(result)
        # for i in range(len(result)):
        #     print(i,result[i])
        for i in range(len(result)):
            # print(result[i])
            # print(model)
            # print(model[result[i][4]])

            result_list.append(
                {'city': result[i][2] + " " + result[i][3], "latitude": result[i][0], "longitude": result[i][1],
                 "data-total": model[str(result[i][4])][0],
                 "data-new": model[str(result[i][4])][1]})
            count += 1

        time3 = time.time()
        print(time2 - time1, time3 - time2)
        conn.close()
        result = {"status": "success", "count": count, "data_list": result_list, "state_coord": STATE_COORD[selected_state]}
        # print(len(metro_list), len(result_lat_list), len(result_lon_list), len(result_data))
        # for i in range(len(metro_list)):
        #     print(i,metro_list[i])
        # print(result)
        return result


def get_data(date, map, selected_state="all"):
    #print("map", map)
    if map == "confirmed_county":
        return get_confirmed_county_data(date,selected_state)
    if map == "confirmed":
        return get_confirmed_data(date,selected_state)
    elif map == "labeled":
        return get_confirmed_county_data(date,selected_state)
    elif map == "predicted":
        return get_lowlevel_data(date,selected_state)
    elif map == "heatmap":
        # print("heatmap", get_heat_point(date, selected_state))
        return get_heat_point(date, selected_state)


def get_heat_point(date,state):
    with open("./Data/coordinates/%s"%(state), "rb") as f:
    # with open(date + state + ".csv", "r") as f:
        coordinates = pickle.load(f)
    with open("./Data/heatmap/%s_%s"%(state,date), "rb") as f:
    # with open(date + state + ".csv", "r") as f:
        heatdata = pickle.load(f)
    #print(heatdata)
    heatdata = np.divide(heatdata,2)
    result = np.append(heatdata[:, 1:2], heatdata[:, 0:1], axis=1)
    result = np.append(result, heatdata[:, 2:3], axis=1)
    # print(result)
    lat = list(coordinates[:, 1].reshape(1, -1)[0].astype("float64"))
    lon = list(coordinates[:, 0].reshape(1, -1)[0].astype("float64"))
    #print(shape(weight))
    weight = list(heatdata.reshape(1, -1)[0].astype("float64"))

    return {"status": "success", "count": result.shape[0], "latitude": lat, "longitude": lon, "weight": weight,  "state_coord": STATE_COORD[state], "lat_len":len(lat), "lon_len":len(lon)}

# standard style
# publication
# jhu data source link
# 准确 及时
#

def get_lowlevel_data(date, selected_state):
    """
       date:date
       map:maptype
       selected_state:selected_state to display
       return: a dict with status, count, metro list, latitude list, longtitude list,
       """
    # print(selected_state)
    if date is None:
        return {"status": "unsuccess"}
    else:
        if selected_state not in ["all states", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
                                  "IL", "IN", "IA", "KS", "KY", "LA",
                                  "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC",
                                  "ND", "OH", "OK",
                                  "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY","DC"]:
            selected_state = "all states"
        # print(selected_state)
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database='us_metro',
            autocommit=True,
            charset='utf8')
        cursor = conn.cursor()
        time1 = time.time()
        with open("./Data/predicted/%s.json" % date, 'r', encoding='utf-8') as json_file:
            model = json.load(json_file)
        time2 = time.time()
        result_list = []
        single_list = []
        count = 0
        fips_list = list(model.keys())
        # for i in model:
        #     # states = i[-len(i) + i.rfind(" ") + 1:].split("-")
        #     # cities = i[:-len(i) + i.rfind(" "):1].split("-")
        #     # print(i,len(i)-i.rfind(" "),cities,states,)
        #     # print(i, len(i) - i.rfind(" "), cities, states, selected_state)
        #     #single_list.append([cities[0], states[0]])
        #
        #         # for multiple county in one row
        #         else:
        #             latlist = []
        #             lonlist = []
        #             lat, lon = get_data_multi(cities, states, cursor)
        #             if lat != 0 and lon != 0:
        #                 # latitude = sum(latlist)/len(latlist)
        #                 # longtitue = sum(lonlist)/len(lonlist)
        #                 # print(latitude,longtitue)
        #                 # resultlist[str(count)]=[i,latitude,longtitue,model[i]]
        #                 result_list.append({'city': i, "latitude": lat, "longitude": lon, "data": model[i]})
        #                 count += 1
        #             else:
        #                 print(i, cities, states)

        # for single county (this increases speed of searching)
        sql = 'select latitude,longitude,city,state from metro_fips where'
        if selected_state == "all states":
            for i in range(len(fips_list)-1):
                sql += ' (fips = %s ) or' % (fips_list[i])
            sql += ' (fips = %s)' % (fips_list[len(fips_list)-1])

        else:
            for i in range(len(fips_list)-1):
                sql += ' (fips = %s and state like "%%%s%%") or' % (fips_list[i],selected_state)
            sql += ' (fips = %s and state like "%%%s%%")' % (fips_list[len(fips_list)-1],selected_state)
        #single_list[len(single_list) - 1][0], single_list[len(single_list) - 1][1])
        cursor.execute(sql)
        result = cursor.fetchall()
        # for i in range(len(result)):
        #     print(i,result[i])
        for i in range(len(result)):
            result_list.append(
                {'city': result[i][2] + " " + result[i][3], "latitude": result[i][0], "longitude": result[i][1],
                 "data": model[fips_list[i]]})
            count += 1

        time3 = time.time()
        print(time2 - time1, time3 - time2)
        conn.close()
        result = {"status": "success", "count": count, "data_list": result_list, "state_coord": STATE_COORD[selected_state]}
        # print(len(metro_list), len(result_lat_list), len(result_lon_list), len(result_data))
        # for i in range(len(metro_list)):
        #     print(i,metro_list[i])
        # print(result)
        return result


if __name__ == "__main__":
    # time1 = time.time()
    #print(get_confirmed_data("2021-11-10","AK"))
    # print(time.time()-time1)
    #print(get_heat_point("40","output"))

    print(get_heat_point("2022-01-08", ))



