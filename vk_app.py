import requests
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

scope = 0
time_bank = 0


def reader(name):
    with open(name, 'r') as f:
        return f.read()


token = reader("t.txt")
id = reader("id.txt")
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
                time.sleep(0.7)
            time_bank = second_time_stamp
        # конец проверочного блока
        try:
            response = requests.get(url=url)
            scope += 1
            return response.json()['response']

        except Exception:
            print("возникли проблемы с соединением, проверьте соединение, сообщение от сервера:",
                  response.json()['error']['error_msg'])
            time.sleep(5)


while True:
    # получае список id групп
    groups = get_response(base_url, version, token, "groups.get", "user_id={}&filter=admin".format(id))["items"]
    # получае список id членов группы
    members = []
    for g_id in groups:
        list_of_members = get_response(base_url, version, token, "groups.getMembers", "group_id={}".format(g_id))[
            'items']
        for mem in list_of_members:
            members.append([mem, g_id])

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

    # приводим свединя о групах и их id в удобный вид



    # получаем статистику групп,организовывая полученные даные в словарь,с Id группы в качестве ключа и статистикой в качестве значения
    """
    groups_stats = []
    for g_id in groups:
        groups_stats.append(get_response(base_url, version, token, "stats.get",
                                         "group_id={}&extended=1&stats_groups= visitors, reach, activity&timestamp_from={}&timestamp_to={}".format(
                                             g_id, int(time.time()) - 7200, int(time.time())))[0])
    print(groups_stats)
    #приводим информацию о статистике в удобный вид
    for g in groups_stats:
        g['reach'] = g['reach']['reach_subscribers']
        g['visitors'] = g['visitors']['visitors']

    print(groups_stats)
    """
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
            post_dict[properties] = post[properties]
        post_dict['comments'] = post['comments']['count']
        post_dict['reposts'] = post['reposts']['count']
        post_dict['likes'] = post['likes']['count']
        if 'copy_history' in post.keys():
            post_dict['id'] = post['copy_history'][0]['id']
            post_dict['photo'] = post['copy_history'][0]['attachments'][0]['photo']['sizes'][4]['url']
        else:
            post_dict['id'] = post['id']
            post_dict['photo'] = post['attachments'][0]['photo']['sizes'][4]['url']

        new_posts_list.append(post_dict)

    engine = create_engine("postgresql+psycopg2://postgres:{}@localhost/vk_db".format(reader("postpas.txt")))
    # сгружаем таблицу с группами в psql
    pd.DataFrame(groups_info_list,
                 columns=["group_id", "group_name", "is_closed", "group_type", "is_advertiser", "group_photo"]).to_sql(
        'groups_info_table', con=engine, if_exists='replace', index=False)
    # сгружаем таблицу с информацией о постах в psql
    pd.DataFrame(new_posts_list).to_sql('posts_table', con=engine, if_exists='append', index=False)
    # сгружаем таблицу с id подписчиков групп
    pd.DataFrame(members, columns=["subscribers_id", "group_id"]).to_sql('members_table', con=engine,
                                                                         if_exists='replace', index=False)
    time.sleep(7200)
