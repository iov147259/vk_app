import requests
import time
import pandas as pd
import datetime
from sqlalchemy import create_engine

scope = 0
time_bank = 0


def to_date(unix_time):
    buffer = str(time.ctime(unix_time)).split()
    return (str(buffer[2]) + ' ' + str(buffer[1]) + " " + str(buffer[4]))


def reader(name):
    with open(name, 'r') as f:
        return f.read()


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


while True:
    # получае список id групп
    groups = get_response(base_url, version, token, "groups.get", "user_id={}&filter=admin".format(id))["items"]
    # получае список c id и количеством подписчиков группы
    members = []
    for g_id in groups:
        count_of_members = get_response(base_url, version, token, "groups.getMembers", "group_id={}".format(g_id))[
            'count']
        members.append([g_id, count_of_members])

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
        groups_info_list.append(list(g.values()))

    # получаем статистику групп, котрая имеется до этого момента(исполняется один раз)
    if scope == 5:
        groups_stats = []
        groups_stats_list = []
        for g_id in groups:
            groups_stats_list.append([g_id, get_response(base_url, version, token, "stats.get",
                                                         "group_id={}&extended=1&interval = day&stats_groups= visitors, reach, activity&timestamp_from={}&timestamp_to={}".format(
                                                             g_id, int(time.time()) - 1036800, int(time.time())))])

        for period_num in reversed(range(len(groups_stats_list[0][1]))):
            for group_list in groups_stats_list:

                if 'activity' in group_list[1][period_num].keys():
                    groups_stats.append(
                        [group_list[0], to_date(group_list[1][period_num]["period_from"])] + to_arr_of_active(list(
                            group_list[1][period_num]['activity'].items())))
                else:

                    groups_stats.append(
                        [group_list[0], to_date(group_list[1][period_num]["period_from"]), 0, 0, 0, 0, 0])
        for members_arr in members:
            members_arr[1] = 0
            for stats in groups_stats:
                if stats[0] == members_arr[0]:
                    stats.append(members_arr[1]+int(stats[3]) - int(stats[4]))
                    members_arr[1] = members_arr[1] + int(stats[3]) - int(stats[4])
        stats_column_names = ["group_id", "date", "likes", "subscribed", " unsubscribed", "comments", "reposts",
                              "count of subscribers"]
        engine = create_engine("postgresql+psycopg2://postgres:{}@localhost/vk_db".format(reader("postpas.txt")))
        print("processing...")
        pd.DataFrame(groups_stats, columns=stats_column_names).to_sql('stats_table', con=engine,
                                                                      if_exists='replace', index=False)
        print("done!")

    # получаем данные о постах сообщества
    groups_posts = []
    for group_id in groups:
        groups_posts += get_response(base_url, version, token, "wall.get",
                                     "owner_id=-{}&count=20&extended=1&fields= id, name".format(group_id))['items']
    # приводим данные о постах в удобоваримыый вид
    list_of_properties = ['from_id', 'date', 'post_type', 'text']
    new_posts_list = []
    for post in groups_posts:
        post_dict = {}
        for properties in list_of_properties:
            if properties != 'date':
                post_dict[properties] = post[properties]
            else:
                post_dict['date of publication '] = to_date(post[properties])

        post_dict['comments'] = post['comments']['count']
        post_dict['reposts'] = post['reposts']['count']
        post_dict['likes'] = post['likes']['count']
        if 'copy_history' in post.keys():

            post_dict['photo'] = post['copy_history'][0]['attachments'][0]['photo']['sizes'][4]['url']
        else:

            post_dict['photo'] = post['attachments'][0]['photo']['sizes'][4]['url']

        new_posts_list.append(post_dict)

    engine = create_engine("postgresql+psycopg2://postgres:{}@localhost/vk_db".format(reader("postpas.txt")))
    # сгружаем таблицу с группами в psql
    print("processing...")
    pd.DataFrame(groups_info_list,
                 columns=["group_id", "group_name", "is_closed", "group_type", "is_advertiser", "group_photo"]).to_sql(
        'groups_info_table', con=engine, if_exists='replace', index=False)
    # сгружаем таблицу с информацией о постах в psql
    pd.DataFrame(new_posts_list).to_sql('posts_table', con=engine, if_exists='replace', index=False)
    # сгружаем таблицу с id подписчиков групп
    pd.DataFrame(members, columns=["subscribers_id", "group_id"]).to_sql('members_table', con=engine,
                                                                         if_exists='replace', index=False)
    print("done!")

    time.sleep(7200)
