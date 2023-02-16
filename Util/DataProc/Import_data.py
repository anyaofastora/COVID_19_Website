import pymysql
import pandas as pd
import numpy as np
# df = pd.read_excel("uscities.xlsx")
# print(df)
# data_row = df.iloc[2,:].values
# print(data_row[2])
conn = pymysql.connect(
    host='localhost',
    user='root', password='LRc19980307',
    database='us_metro',
    charset='utf8')
cursor = conn.cursor()
count = 10172
# sql = """
# CREATE TABLE USER1 (
# id INT auto_increment PRIMARY KEY ,
# name CHAR(10) NOT NULL UNIQUE,
# age TINYINT NOT NULL
# )ENGINE=innodb DEFAULT CHARSET=utf8;  #注意：charset='utf8' 不能写成utf-8
# """


def search_column(cursor, table):
    """
    查找数据库所有属性名称
    :param table: 数据库
    :param cursor:指针
    :return:全属性列表（按设计顺序排列）
    """

    sql = "select *from information_schema.COLUMNS where table_name = 'metro' ORDER BY ordinal_position;"
    col = []
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        col.append(row[3])
    # print("searchcol", col)
    return col


def get_comment_dict(cursor, table="compl"):
    """
    查找数据库所有属性的注释
    :param cursor:
    :return:
    """
    if table == "compl":
        cursor.execute('select column_name,column_comment from information_schema.COLUMNS where TABLE_NAME=%s',
                       "pepperdata")
    else:
        cursor.execute('select column_name,column_comment from information_schema.COLUMNS where TABLE_NAME=%s',
                       "pepperbriefdata")
    # print("col", get_columns(cursor2,"pepperdata"))
    return_columns = cursor.fetchall()
    # print(return_columns)
    col_dict = {}
    for col in return_columns:
        col_dict[col[0]] = col[1]
    return col_dict


def get_coord_multi(cities, states, cursor):

    lat = []
    lon = []
    for city in cities:
        for state in states:
            sql = 'select latitude,longtitude from metro where city="%s" and state_id="%s";' % (city, state)
            cursor.execute(sql)
            result = cursor.fetchall()
            print(result,city,state)
            if result != ():
                # print(sql)
                lat.append(result[0][0])
                lon.append(result[0][1])
                # latlist.append(result[0][0])
                # lonlist.append(result[0][1])
    if len(lat) != 0:
        return sum(lat)/len(lat),sum(lon)/len(lon)
    else:
        print("not found",cities,states)
        return 0,0


#
# names = search_column(cursor,"compl")
# col_dict = get_comment_dict(cursor,"compl")
# print("names",names)
# for i in range(len(names)):
#     print(i,names[i],col_dict[names[i]])


#names = ['city', 'city_ascii', 'state_id', 'state_name', 'latitude', 'longtitude', 'population', 'density', 'id',"fips"]
names = ["fips","city","state","latitude","longitude"]
def insert(row):
    sqlcmd = "INSERT INTO metro_fips ("
    for k in range(len(names)-1):
        sqlcmd = sqlcmd + str(names[k]) + ", "
    sqlcmd = sqlcmd + str(names[len(names)-1])+") VALUES ('"
    for k in range(len(row)-1):
        row[k] = str(row[k]).replace("'", "\\'")
        sqlcmd = sqlcmd+row[k] + "','"
    row[len(row)-1] = str(row[len(row)-1]).replace("'", "\\'")
    sqlcmd = sqlcmd + row[len(row)-1] + "');"
    print(sqlcmd)
    cursor.execute(sqlcmd)
    conn.commit()



# df = pd.read_excel("C:/Users/Li Runchao/PycharmProjects/pepper/2000-2010.xlsx")
# for i in range(1,173):
#     data_row = df.iloc[i,:].values
#     insert(data_row,count,2000)
#     count += 1
#
# df = pd.read_excel("C:/Users/Li Runchao/PycharmProjects/pepper/2010-2020.xlsx")
# for i in range(1,219):
#     data_row = df.iloc[i,:].values
#     insert(data_row,count,2010)
#     count += 1
#df = pd.read_excel("C:/Users/Li Runchao/PycharmProjects/pepper/2018.xlsx")
# for i in range(1,42):
#     data_row = df.iloc[i,:].values
#     insert_test_data(data_row,count,2018)
#     count += 1

# df = pd.read_excel("C:/Users/Li Runchao/PycharmProjects/pepper/2020.xlsx")
# for i in range(2,38):
#     data_row = df.iloc[i,:].values
#     insert_test_data(data_row,count,2020)
#     count += 1

df = pd.read_excel("list1_2020.xlsx")
fips = []
for i in range(0,1916):
    data_row = df.iloc[i,:].values
    citystate = data_row[3]
    city = citystate.split(", ")[0]
    state = citystate.split(", ")[1]
    city_list = []
    state_list = []
    if data_row[0] not in fips:
        if "-" in city:
            for i in city.split("-"):
                city_list.append(i)
        else:
            city_list.append(city)
        if "-" in state:
            for i in state.split("-"):
                state_list.append(i)
        else:
            state_list.append(state)
        lat,lon = get_coord_multi(city_list,state_list,cursor)
        fips.append(data_row[0])
        row = [data_row[0],city,state,lat,lon]
        print(row)
        insert(row)


cursor.close()
# 关闭数据库连接
conn.close()