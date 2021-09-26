import pymysql
import pandas as pd
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()

print("DATABASE 연결 중")
con = pymysql.connect(host='tae2089.synology.me',
                      port=51420,
                      user='test',
                      password='test',
                      db='test')
print("DATABASE 연결 완료")

# 디폴트 커서 생성
cur = con.cursor()

sql = "SELECT my_champ, win, champ_lane, kills, assists, deaths, total_minions_killed, champ_level from test.overall "
cur.execute(sql)
rows = list(cur.fetchall())
con.commit()

print(rows[1])
col_name = ['my_champ', 'win_lose', 'champ_lane', 'kills', 'assists', 'deaths', 'total_minions_killed', 'champ_level']
list_df = pd.DataFrame(rows, columns=col_name)
#list_df = list_df.drop(['game_duration', 'match_id'], axis=1)
#count1 = list_df.groupby([list_df['my_champ'], list_df['win']]).size()
# print(count1)
# count2 = pd.DataFrame(count1)
# print(count2)
print(list_df)

count1 = list_df.groupby([list_df['my_champ'], list_df['champ_lane']])

print(type(count1))
count2 = count1.mean()
print(count2)

count3 = count2.reset_index()

count3 = round(count3,1)
print(count3)

sql = "select DISTINCT(my_champ) from test.overall"
cur.execute(sql)
champ_ids = list(cur.fetchall())
con.commit()

champ_ids2 = []
for i in champ_ids:
    champ_num = i[0]
    champ_ids2.append(champ_num)
champ_ids2.sort()
print(champ_ids2)

test = list_df.groupby(['my_champ', 'champ_lane', 'win_lose']).size()
test2 = test.to_frame(name='count')

wltotal = []
wintotal = []
losetotal = []
lanes = ['BOTTOM', 'JUNGLE', 'MIDDLE', 'NONE', 'TOP']
winorlose = ['Win', 'Lose']

for p in champ_ids2:
    champ_num = p
    #     print(i)
    #     i = i.replace('(', '').replace(')', '').replace(',','')
    #     i = int(i)
    for lane in lanes:
        # for wl in winorlose:
        try:
            lose = test2['count'][champ_num][lane]['Lose']
        except:
            lose = 0

        try:
            win = test2['count'][champ_num][lane]['Win']
        except:
            win = 0

        if win == 0:
            if lose == 0:
                continue

        wl = win + lose

        wltotal.append(wl)
        wintotal.append(win)
        losetotal.append(lose)
        print(lane)
        print(win)
        print(lose)
        print(wl)
        print(i)
        print('')
        # i+=1
print(wltotal)

count3['win+lose'] = wltotal
count3['win'] = wintotal
count3['lose'] = losetotal

count3['winrate'] = round(count3['win']/count3['win+lose'], 3)

#champ_ids2.sort()

engine = create_engine("mysql+mysqldb://test:"+"test"+"@tae2089.synology.me:51420/test", encoding='utf-8')
conn = engine.connect()
count3.to_sql(name = 'champ_lane_mean2', con=engine, if_exists='replace', index=False)

con.close()
conn.close()
