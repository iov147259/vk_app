import requests
import time
import pandas as pd
import datetime

from sqlalchemy import create_engine
import psycopg2

scope = 0
time_bank = 0
flag = 0


def to_date(unix_time):
    buffer = str(time.ctime(unix_time)).split()
    return (str(buffer[2]) + ' ' + str(buffer[1]) + " " + str(buffer[4]))


def reader(name):
    with open(name, 'r') as f:
        return f.read()


engine = create_engine("postgresql+psycopg2://postgres:{}@localhost/vk_db".format(reader("postpas.txt")))
con = psycopg2.connect(database="vk_db", user='postgres', password=reader("postpas.txt"), host="localhost",
                       port=5432)
token = reader("t2.txt")

base_url = "https://api.vk.com/method/{}?{}&access_token={}&v={}"
version = "5.130"



def get_response(base_url, version, token, method, params):
    url = base_url.format(method, params, token, version)

    while True:
        # блок проврки, не было ли более 3-х запросов в секунду
        global scope, time_bank
        if scope == 0:
            time_bank = time.time()
        if scope % 3 == 0 and scope != 0:
            second_time_stamp = time.time()

            if second_time_stamp - time_bank <= 1:
                time.sleep(0.8)
            time_bank = second_time_stamp
        # конец проверочного блока
        try:
            print("connecting...")
            response = requests.get(url=url)
            scope += 1
            print(" done!")
            if 'error' not in response.json().keys():
                return response.json()
            print("возникли проблемы", response.json()['error']['error_msg'])
            time.sleep(3)
            break



        except Exception:
            if response.json()['error']['error_code'] == 15:
                print("доступ запрещён")

            else:
                print("возникли проблемы с соединением, проверьте соединение, сообщение от сервера:",
                      response.json()['error']['error_msg'])
                time.sleep(10)


print("waking up...")
#получаем id пользователя,которому пренадлежит токен
info=get_response(base_url, version, token,"account.getProfileInfo",'')['response']
id=info['id']


# получаем список групп
groups = get_response(base_url, version, token, "groups.get", "user_id={}&filter=admin".format(id))['response']["items"]
# получае список c id и количеством подписчиков группы
# обрабатываем истории
stories_list = []
for g in groups:

    s = get_response(base_url, version, token, "stories.get",
                     "owner_id={}&extended=1".format(g * (-1)))["response"]["items"]

    if s:
        for st in s[0]['stories']:
            if "photo" in st:
                stories_list.append(
                    {"stories_id": st['id'], "group_id": st['owner_id'], "date_of_publication": datetime.date.fromtimestam(st["date"]),
                     "image/photo": st['photo']['sizes'][-1]['url']})
            else:
                stories_list.append(
                    {"stories_id": st['id'], "group_id": st['owner_id'], "date_of_publication": datetime.date.fromtimestamp(st["date"]),
                     "image/photo": st["video"]['image'][-1]['url']})

cur = con.cursor()
columns=['stories_id','group_id',"date_of_publication","image/photo"]
try:
    cur.execute("SELECT * FROM stories_table")
    f = cur.fetchall()
except Exception:
    f = False

if not f and (f != []):

    pd.DataFrame(stories_list, columns=columns).to_sql(
        'stories_table', con=engine, if_exists='replace', index=False)

else:

    ids_list = [[stor[0], stor[1]] for stor in f]

    stories_list_filtered = []
    for story in stories_list:
        if [story["stories_id"], story['group_id']] not in ids_list:
            stories_list_filtered.append(story)

    pd.DataFrame(stories_list_filtered, columns=columns).to_sql(
        'stories_table', con=engine, if_exists='append', index=False, )

# обрабатываем статистику историй
story_stats = []
columns_stats=["story id", "group id",'answer by story','shares','subscribed','views','likes',"timestamp"]
for stories in stories_list:
    stat = get_response(base_url, version, token, "stories.getStats",
                        "owner_id={}&story_id={}".format(stories['group_id'], stories['stories_id']))['response']

    story_stats.append({"story id": stories['stories_id'], "group id": stories['group_id'],
                        'answer by story': stat['replies']['count'], 'shares': stat["shares"]['count'],
                        'subscribed': stat['subscribers']['count'], 'views': stat['views']["count"],
                        'likes': stat["likes"]['count'], "timestamp": datetime.datetime.fromtimestamp(time.time())})
try:
    cur.execute("SELECT * FROM story_stats")
    f = cur.fetchall()

except Exception:
    f = False
if not f:
    pd.DataFrame(story_stats, columns=columns_stats).to_sql(
        'story_stats', con=engine, if_exists='append', index=False, )
else:
    stats_story = [[s['story id'], s['group id']] for s in story_stats]
    for story in f:
        if [story[0], story[1]] not in stats_story:
            print(1)
            story_stats.append({"story id": story[0], "group id": story[1],
                                'answer by story': story[2], 'shares': story[3],
                                'subscribed': story[4], 'views': story[5],
                                'likes': story[6], "timestamp": datetime.datetime.fromtimestamp(time.time())})

    cur.execute("DROP TABLE story_stats")
    con.commit()
    print(story_stats)
    pd.DataFrame(story_stats, columns=columns_stats).to_sql(
        'story_stats', con=engine, if_exists='replace', index=False)
print("slipping now...")
con.close()
