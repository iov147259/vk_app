import numpy
import requests
import time
import pandas as pd
import datetime

from sqlalchemy import create_engine
import psycopg2

scope = 0
time_bank = 0
flag = 0
statrt_date_for_stats = datetime.datetime(2020, 1, 1).timestamp()


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
    time.sleep(1)
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
# получаем id пользователя,которому пренадлежит токен
info = get_response(base_url, version, token, "account.getProfileInfo", '')['response']
id = info['id']

# получаем список групп
groups = get_response(base_url, version, token, "groups.get", "user_id={}&filter=admin".format(id))['response']["items"]
# получае список c id и количеством подписчиков грgroups.getуппы
members = []
for g_id in groups:
    count_of_members = \
        get_response(base_url, version, token, "groups.getMembers", "group_id={}".format(g_id))['response'][
            'count']
    members.append([g_id, count_of_members - 1])

# получаем данные о группах

groups_info = get_response(base_url, version, token, "groups.getById",
                           "group_ids={}".format(','.join([str(strs) for strs in groups])))['response']

# удаляем лишние данные
groups_info_list = []
for g in groups_info:
    del g['photo_50']
    del g['photo_100']
    del g['screen_name']
    del g['is_admin']
    del g['admin_level']
    del g['is_member']
    g["group link"] = "https://vk.com/club{}".format(g["id"])
    groups_info_list.append(g.values())

# проверяем есть ли таблица со статистикой, если таковой нет - грузим историческую инфу
cur = con.cursor()
try:
    cur.execute("SELECT * FROM stats_table")
    f = cur.fetchone()
except Exception:
    f = False

if not bool(f):
    groups_stats = []
    groups_stats_list = []
    for g_id in groups:
        groups_stats_list.append([g_id, get_response(base_url, version, token, "stats.get",
                                                     "group_id={}&extended=1&interval = day&stats_groups= visitors, reach, activity&timestamp_from={}&timestamp_to={}".format(
                                                         g_id, statrt_date_for_stats, int(time.time())))[
            'response']])

    for period_num in range(len(groups_stats_list[0][1])):

        for group_list in groups_stats_list:

            if 'activity' in group_list[1][period_num].keys():
                groups_stats.append(
                    [group_list[0],
                     datetime.datetime.fromtimestamp(group_list[1][period_num]["period_from"])] + to_arr_of_active(list(
                        group_list[1][period_num]['activity'].items())))

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
# если таблица с инфой уже существует, грузим в неё данные
else:
    groups_stats = []
    groups_stats_list = []
    for g_id in groups:
        groups_stats_list.append([g_id, get_response(base_url, version, token, "stats.get",
                                                     "group_id={}&extended=1&interval = day&stats_groups= visitors, reach, activity&timestamp_from={}&timestamp_to={}".format(
                                                         g_id, int(time.time()) - 7200, int(time.time())))['response']])

    for period_num in range(len(groups_stats_list[0][1])):
        for group_list in groups_stats_list:

            if 'activity' in group_list[1][period_num].keys():
                groups_stats.append(
                    [group_list[0], datetime.datetime.fromtimestamp(time.time())] + to_arr_of_active(list(
                        group_list[1][period_num]['activity'].items())))

    for members_arr in members:

        for stats in groups_stats:
            if stats[0] == members_arr[0]:
                stats.append(members_arr[1])

    stats_column_names = ["group_id", "date", "likes", "subscribed", " unsubscribed", "comments", "reposts",
                          "count of subscribers"]
    cur.execute("SELECT * FROM stats_table")
    last_date = cur.fetchall()[-1][1]

    if ((groups_stats != []) and (groups_stats[-1][1] == last_date)):
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
posts_photos = []

for group_id in groups:
    last_post = -1
    bufer = [1, 2]
    counter = 0
    while len(bufer) != 1 and len(bufer) != 0:

        bufer = get_response(base_url, version, token, "wall.get",
                             "owner_id=-{}&count=100&offset={}&extended=1&fields= id, name".format(group_id, counter))[
            'response']['items']
        for item in bufer:
            item.update({"group_id": '-' + str(group_id)})
        groups_posts += bufer
        counter += 100

# приводим данные о постах в удобоваримыый вид
list_of_properties = ["group_id", 'date', 'post_type', 'text']
new_posts_list = []
for post in groups_posts:
    post_dict = {}
    for properties in list_of_properties:
        if properties != 'date':
            post_dict[properties] = post[properties]
        else:
            post_dict['date of publication '] = datetime.date.fromtimestamp(post[properties])
    post_dict["post id"] = post['id']
    post_dict['post link'] = "https://vk.com/wall{}_{}".format(post_dict["group_id"], post_dict['post id'])
    post_dict['comments'] = post['comments']['count']
    post_dict['reposts'] = post['reposts']['count']
    post_dict['likes'] = post['likes']['count']
    if 'copy_history' in post.keys():
        for item in post['copy_history'][0]['attachments']:
            if "photo" in item.keys():
                posts_photos.append([post_dict["group_id"], post_dict['post id'], item['photo']['sizes'][4]['url']])




    elif 'attachments' in post.keys():
        arr = []
        for item in post['attachments']:
            if "photo" in item.keys():
                posts_photos.append([post_dict["group_id"], post_dict['post id'], item['photo']['sizes'][4]['url']])

    new_posts_list.append(post_dict)
print("loading in plsq...")
photos_column = ["goup_id", "post_id", "photo"]

try:
    cur.execute("SELECT * FROM posts_photos")
    f = cur.fetchall()
except Exception:
    f = -1

if f != -1:
    a = [list(i) for i in f if list(i) not in posts_photos]

    if a:
        posts_photos = posts_photos + a

# сгружаем таблицу с информацией о постах в psql
try:
    cur = con.cursor()
    cur.execute("SELECT * FROM posts_table")

    f = cur.fetchall()

except Exception:
    f = -1

if f != -1:

    a = [{'group_id': i[0], 'date of publication ': i[1], 'post_type': i[2], 'text': i[3], 'post id': i[4],
          'post link': i[5], 'comments': i[6], 'reposts': i[7], 'likes': i[8]} for i in f if
         {'group_id': i[0], 'date of publication ': i[1], 'post_type': i[2], 'text': i[3], 'post id': i[4],
          'post link': i[5], 'comments': i[6], 'reposts': i[7], 'likes': i[8]} not in new_posts_list]
    if a:
        new_posts_list = new_posts_list + a
con.close()

pd.DataFrame(posts_photos,
             columns=photos_column).to_sql(
    'posts_photos', con=engine, if_exists='replace', index=False)
pd.DataFrame(groups_info_list,
             columns=["group_id", "group_name", "is_closed", "group_type", "is_advertiser", "group_photo",
                      "group_link"]).to_sql(
    'groups_info_table', con=engine, if_exists='replace', index=False)
pd.DataFrame(new_posts_list).to_sql('posts_table', con=engine, if_exists='replace', index=False)

print("done!")

# здесь получаем и обрабатываем  статистику по постам

posts_stats = []

for groups_id in groups:
    list_of_post = [str(i["post id"]) for i in new_posts_list if
                    i['group_id'] == "-" + str(groups_id)]
    list_of_metrics = [[str(i["comments"]), str(i["reposts"]), str(i["likes"])] for i in new_posts_list if
                       i['group_id'] == "-" + str(groups_id)]
    count_of_parts = int(len(list_of_post) / 30) + 1
    parted_list = numpy.array_split(numpy.array(list_of_post), count_of_parts)
    parted_metrics_list = numpy.array_split(numpy.array(list_of_metrics), count_of_parts)

    for part_number in range(count_of_parts):

        try:

            post_stat = get_response(base_url, version, token, "stats.getPostReach",
                                     "owner_id={}&post_ids={}".format("-" + str(groups_id),
                                                                      ",".join(parted_list[part_number])))['response']

            for number in range(len(post_stat)):
                posts_stats.append([post_stat[number].get('post_id', 0), post_stat[number].get('reach_subscribers', 0),
                                    post_stat[number].get('reach_total', 0), post_stat[number].get('reach_viral', 0),
                                    post_stat[number].get('reach_ads', 0),
                                    post_stat[number].get('to_group', 0), post_stat[number].get('unsubscribe', 0),
                                    int(parted_metrics_list[part_number][number][0]),
                                    int(parted_metrics_list[part_number][number][1]),
                                    int(parted_metrics_list[part_number][number][2]),
                                    datetime.date.fromtimestamp(time.time())])




        except Exception:

            continue

post_columns_names = ["post_id", "reach_subscribers", "reach_total", "viral_reach", "reach_ads",
                      "mowing to group", "unsubscribe ", "comments", "reposts", "likes", "stats_date"]

try:
    con = psycopg2.connect(database="vk_db", user='postgres', password=reader("postpas.txt"), host="localhost",
                         port=5432)
    cur = con.cursor()
    cur.execute("SELECT * FROM groups_posts_stats")
    last_date = cur.fetchall()[-1][-1]
    print(last_date)
except Exception:
    last_date = None
if posts_stats != []:
    if last_date is None:
        print('creating...')

        pd.DataFrame(posts_stats,
                     columns=post_columns_names).to_sql(
            'groups_posts_stats', con=engine, if_exists='append', index=False)


    elif str(posts_stats[-1][-1]) != str(last_date):
        print("adding...")
        pd.DataFrame(posts_stats,
                     columns=post_columns_names).to_sql(
            'groups_posts_stats', con=engine, if_exists='append', index=False)
    else:
        print("updating...")
        with con as con:
            cur = con.cursor()
            cur.execute("DELETE FROM groups_posts_stats WHERE stats_date = %s", (last_date,))
            con.commit()
        pd.DataFrame(posts_stats,
                     columns=post_columns_names).to_sql(
            'groups_posts_stats', con=engine, if_exists='append', index=False)

        print("update!")
elif last_date == None:
    pd.DataFrame(posts_stats,
                 columns=post_columns_names).to_sql(
        'groups_posts_stats', con=engine, if_exists='append', index=False)

print("slipping now...")
