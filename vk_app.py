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
# получаем список групп
groups = get_response(base_url, version, token, "groups.get", "user_id={}&filter=admin".format(id))['response']["items"]
# получае список c id и количеством подписчиков группы
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
                                                         g_id, int(time.time()) - 1900800, int(time.time())))[
            'response']])

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
posts_photos = []
for group_id in groups:
    bufer = get_response(base_url, version, token, "wall.get",
                         "owner_id=-{}&count=20&extended=1&fields= id, name".format(group_id))['response']['items']
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
            posts_photos.append([post_dict["group_id"], post_dict['post id'], item['photo']['sizes'][4]['url']])



    elif 'attachments' in post.keys():
        arr = []
        for item in post['attachments']:
            posts_photos.append([post_dict["group_id"], post_dict['post id'], item['photo']['sizes'][4]['url']])

    new_posts_list.append(post_dict)
print("loading in plsq...")
photos_column = ["goup_id", "post_id", "photo"]
pd.DataFrame(posts_photos,
             columns=photos_column).to_sql(
    'posts_photos', con=engine, if_exists='replace', index=False)
pd.DataFrame(groups_info_list,
             columns=["group_id", "group_name", "is_closed", "group_type", "is_advertiser", "group_photo",
                      "group_link"]).to_sql(
    'groups_info_table', con=engine, if_exists='replace', index=False)
# сгружаем таблицу с информацией о постах в psql
pd.DataFrame(new_posts_list).to_sql('posts_table', con=engine, if_exists='replace', index=False)

print("done!")
print("slipping now...")

# здесь получаем и обрабатываем  статистику по постам

posts_stats = []
for post in new_posts_list:
    try:
        post_stat = get_response(base_url, version, token, "stats.getPostReach",
                                 "owner_id={}&post_ids={}".format(post['group_id'], post['post id']))['response']

        del post_stat['join_group']
        del post_stat['report']
        del post_stat['hide']
        posts_stats.append(post_stat.values().append(to_date(time.time())))
    except Exception:
        continue

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

# обрабатываем истории
stories_list = []
for g in groups:

    s = get_response(base_url, version, token, "stories.get",
                     "owner_id={}&extended=1".format(g * (-1)))["response"]["items"]

    if s:
        for st in s[0]['stories']:
            if "photo" in st:
                stories_list.append(
                    {"stories_id": st['id'], "group_id": st['owner_id'], "date_of_publication": to_date(st["date"]),
                     "image/photo": st['photo']['sizes'][-1]['url']})
            else:
                stories_list.append(
                    {"stories_id": st['id'], "group_id": st['owner_id'], "date_of_publication": to_date(st["date"]),
                     "image/photo": st["video"]['image'][-1]['url']})

cur = con.cursor()
try:
    cur.execute("SELECT * FROM stories_table")
    f = cur.fetchall()
except Exception:
    f = False

if not f:
    print(stories_list)
    pd.DataFrame(stories_list).to_sql(
        'stories_table', con=engine, if_exists='replace', index=False, )
else:

    ids_list = [[stor[0], stor[1]] for stor in f]
    stories_list_filtered = []
    for story in stories_list:

        if [story["stories_id"], story['group_id']] not in ids_list:
            stories_list_filtered.append(story)

    pd.DataFrame(stories_list_filtered).to_sql(
        'stories_table', con=engine, if_exists='append', index=False, )

# обрабатываем статистику историй
story_stats = []
for stories in stories_list:
    stat = get_response(base_url, version, token, "stories.getStats",
                        "owner_id={}&story_id={}".format(stories['group_id'], stories['stories_id']))['response']

    story_stats.append({"story id": stories['stories_id'], "group id": stories['group_id'],
                        'answer by story': stat['replies']['count'], 'shares': stat["shares"]['count'],
                        'subscribed': stat['subscribers']['count'], 'views': stat['views']["count"],
                        'likes': stat["likes"]['count'], "timestamp": " ".join(time.ctime().split(" ")[1:4])})
try:
    cur.execute("SELECT * FROM story_stats")
    f = cur.fetchall()
    con.close()
except Exception:
    f = False
if not f:
    pd.DataFrame(story_stats).to_sql(
        'story_stats', con=engine, if_exists='append', index=False, )
else:
    stats_story = [[s['story id'], s['group id']] for s in story_stats]
    for story in f:
        if [story[0], story[1]] not in stats_story:
            story_stats.append({"story id": story[0], "group id": story[1],
                        'answer by story': story[2], 'shares': story[3],
                        'subscribed': story[4], 'views': story[5],
                        'likes': story[6], "timestamp": " ".join(time.ctime().split(" ")[1:4])})
    pd.DataFrame(story_stats).to_sql(
        'story_stats', con=engine, if_exists='replace', index=False)
