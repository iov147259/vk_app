import requests
import time
import pandas as pd

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
token = reader("t.txt")
id = reader("id.txt")
base_url = "https://api.vk.com/method/{}?{}&access_token={}&v={}"
version = "5.130"


def to_arr_of_active(arr_active_tuples):
    result = [0, 0, 0, 0, 0]
    for act in arr_active_tuples:
        if act[0] == "likes":
            result[0] = act[1]
        if act[0] == "subscribed":
            result[1] = act[1]
        if act[0] == "unsubscribed":
            result[2] = act[1]
        if act[0] == "comments":
            result[3] = act[1]
        if act[0] == "copies":
            result[4] = act[1]
    return result


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
            return response.json()['response']

        except Exception:
            print("возникли проблемы с соединением, проверьте соединение, сообщение от сервера:",
                  response.json()['error']['error_msg'])
            time.sleep(5)



print("waking up...")
#получаем список групп
groups = get_response(base_url, version, token, "groups.get", "user_id={}&filter=admin".format(id))["items"]
 # получае список c id и количеством подписчиков группы
members = []
for g_id in groups:
    count_of_members = get_response(base_url, version, token, "groups.getMembers", "group_id={}".format(g_id))[
            'count']
    members.append([g_id, count_of_members - 1])

# получаем данные о группах

groups_info = get_response(base_url, version, token, "groups.getById",
                               "group_ids={}".format(','.join([str(strs) for strs in groups])))

# удаляем лишние данные
groups_info_list = []
for g in groups_info:
    del g['photo_50']
    del g['photo_100']
    del g['screen_name']
    del g['is_admin']
    del g['admin_level']
    del g['is_member']

#проверяем есть ли таблица со статистикой, если таковой нет - грузим историческую инфу
cur = con.cursor()
try:
    cur.execute("SELECT * FROM stats_table")
    f = cur.fetchone()
except Exception:
    f=False

if not bool(f):
    groups_stats = []
    groups_stats_list = []
    for g_id in groups:
        groups_stats_list.append([g_id, get_response(base_url, version, token, "stats.get",
                                                     "group_id={}&extended=1&interval = day&stats_groups= visitors, reach, activity&timestamp_from={}&timestamp_to={}".format(
                                                         g_id, int(time.time()) - 1900800, int(time.time())))])

    for period_num in range(len(groups_stats_list[0][1])):

        for group_list in groups_stats_list:

            if 'activity' in group_list[1][period_num].keys():
                groups_stats.append(
                    [group_list[0], to_date(group_list[1][period_num]["period_from"])] + to_arr_of_active(list(
                        group_list[1][period_num]['activity'].items())))
            else:

                groups_stats.append(
                    [group_list[0], to_date(group_list[1][period_num]["period_from"]), 0, 0, 0, 0, 0])
    for members_arr in members:

        for stats in groups_stats:

            if stats[0] == members_arr[0]:
                stats.append(members_arr[1])
                members_arr[1] = members_arr[1] - int(stats[3]) + int(stats[4])
    stats_column_names = ["group_id", "date", "likes", "subscribed", " unsubscribed", "comments", "reposts",
                          "count of subscribers"]


    engine = create_engine("postgresql+psycopg2://postgres:{}@localhost/vk_db".format(reader("postpas.txt")))
    print("loading in psql...")

    pd.DataFrame(reversed(groups_stats), columns=stats_column_names).to_sql('stats_table', con=engine,
                                                                            if_exists='replace', index=False)

    print("done!")
#если таблица с инфой уже существует, грузим в неё данные
else:
    groups_stats = []
    groups_stats_list = []
    for g_id in groups:
        groups_stats_list.append([g_id, get_response(base_url, version, token, "stats.get",
                                                     "group_id={}&extended=1&interval = day&stats_groups= visitors, reach, activity&timestamp_from={}&timestamp_to={}".format(
                                                         g_id, int(time.time()) - 7200, int(time.time())))])

    for period_num in range(len(groups_stats_list[0][1])):
        for group_list in groups_stats_list:

            if 'activity' in group_list[1][period_num].keys():
                groups_stats.append(
                    [group_list[0], to_date(group_list[1][period_num]["period_from"])] + to_arr_of_active(list(
                        group_list[1][period_num]['activity'].items())))
            else:

                groups_stats.append(
                    [group_list[0], to_date(group_list[1][period_num]["period_from"]), 0, 0, 0, 0, 0])
    for members_arr in members:

        for stats in groups_stats:
            if stats[0] == members_arr[0]:
                stats.append(members_arr[1])

    stats_column_names = ["group_id", "date", "likes", "subscribed", " unsubscribed", "comments", "reposts",
                          "count of subscribers"]
    cur.execute("SELECT * FROM stats_table")
    last_date = cur.fetchall()[-1][1]
    if groups_stats[-1][1] == last_date:
        print("update")
        with con as con:
            cur = con.cursor()
            cur.execute("DELETE FROM stats_table WHERE date= %s", (last_date,))
            con.commit()

        pd.DataFrame(groups_stats, columns=stats_column_names).to_sql('stats_table', con=engine,
                                                                      if_exists='append', index=False)
        print("done!")
        # если принятые данные содержат дату, которой еще не было, принятые данные просто вносятся в таблицу и меняется значение переменной last_date, хранящей дату самой свежей записи
    else:
        print("supplement")
        pd.DataFrame(reversed(groups_stats), columns=stats_column_names).to_sql('stats_table', con=engine,
                                                                                if_exists='append', index=False)

        print("done!")
groups_posts = []
for group_id in groups:
    bufer = get_response(base_url, version, token, "wall.get",
                             "owner_id=-{}&count=20&extended=1&fields= id, name".format(group_id))['items']
    for item in bufer:
        item.update({"group_id": '-' + str(group_id)})
    groups_posts += bufer

# приводим данные о постах в удобоваримыый вид
list_of_properties = ["group_id", 'date', 'post_type', 'text']
new_posts_list = []
for post in groups_posts:
    post_dict = {}
    for properties in list_of_properties:
        if properties != 'date':
            post_dict[properties] = post[properties]
        else:
            post_dict['date of publication '] = to_date(post[properties])
    post_dict["post id"] = post['id']
    post_dict['post link'] = "https://vk.com/wall{}_{}".format(post_dict["group_id"], post_dict['post id'])
    post_dict['comments'] = post['comments']['count']
    post_dict['reposts'] = post['reposts']['count']
    post_dict['likes'] = post['likes']['count']
    if 'copy_history' in post.keys():
        for item in post['copy_history'][0]['attachments']:
            arr.append(item['photo']['sizes'][4]['url'])

        post_dict['photo'] = "                   ".join(arr)

    elif 'attachments' in post.keys():
        arr = []
        for item in post['attachments']:
            arr.append(item['photo']['sizes'][4]['url'])

        post_dict['photo'] = "                ".join(arr)

    new_posts_list.append(post_dict)
print("loading in plsq...")
pd.DataFrame(groups_info_list,
                 columns=["group_id", "group_name", "is_closed", "group_type", "is_advertiser", "group_photo"]).to_sql(
        'groups_info_table', con=engine, if_exists='replace', index=False)
# сгружаем таблицу с информацией о постах в psql
pd.DataFrame(new_posts_list).to_sql('posts_table', con=engine, if_exists='replace', index=False)

print("done!")
print("slipping now...")





    # здесь получаем и обрабатываем  статистику по постам
"""
posts_stats = []
for post in new_posts_list:
    post_stat = get_response(base_url, version, token, "stats.getPostReach",
                                 "owner_id={}&post_ids={}".format(post['from_id'], post['post id']))
    del post_stat['join_group']
    del post_stat['report']
    del post_stat['hide']
    posts_stats.append(post_stat.values().append(to_date(time.time())))

post_columns_names = ["reach", "total_reach", "add_reach", "viral_reach", "links",
                          "subscribe", "unsubscribe ", "stats_date"]
if flag == 0:
    pd.DataFrame(posts_stats,
                     columns=post_columns_names).to_sql(
            'groups_posts_stats', con=engine, if_exists='replace', index=False)
elif posts_stats[0][-1] != last_date[0]:
    print("adding...")
    pd.DataFrame(posts_stats,
                     columns=post_columns_names).to_sql(
            'groups_posts_stats', con=engine, if_exists='append', index=False)
else:
    print("updating...")
     with con as con:
        cur = con.cursor()
        cur.execute("DELETE FROM groups_posts_stats WHERE stats_date = %s", (last_date[0],))
        con.commit()
    pd.DataFrame(posts_stats,
                     columns=post_columns_names).to_sql(
            'groups_posts_stats', con=engine, if_exists='append', index=False)
    last_date[0] = posts_stats[0][-1]
    print("update!")
"""


