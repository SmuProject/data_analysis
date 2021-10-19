import pandas as pd
from anytree import Node, RenderTree, search
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()


def get_win_rate(df):
    if df["win_cnt"] + df["lose_cnt"] == 0:
        return 0
    return round(df["win_cnt"] / (df["win_cnt"] + df["lose_cnt"]), 3)


def get_pick_rate(df):
    return round((df["win_cnt"] + df["lose_cnt"]) / df["play_champ_num"], 3)


df = pd.DataFrame(
    columns=[
        "champion_id",
        "lane",
        "item_1",
        "item_2",
        "item_3",
        "play_champ_num",
        "win_cnt",
    ]
)

df2 = pd.DataFrame(
    columns=[
        "champion_id",
        "lane",
        "item_1",
        "item_2",
        "item_3",
        "play_champ_num",
        "lose_cnt"
    ]
)
engine = create_engine(
    "mysql+mysqldb://test:" + "test" + "@tae2089.synology.me:51420/test",
    encoding="utf-8",
)
conn = engine.connect()
con = pymysql.connect(
    host="tae2089.synology.me", port=51420, user="test", password="test", db="test"
)
print("DATABASE 연결 완료")

cur = con.cursor()

sql = "SELECT winrate_summoner.match_id , win_or_lose, champion_lane, winrate_summoner.champion_id, CAST(item_1 AS CHAR(10)), CAST(item_2 AS CHAR(10)), CAST(item_3 AS CHAR(10)), CAST(item_4 AS CHAR(10)) FROM winrate_summoner RIGHT JOIN coreitems ON winrate_summoner.match_id = coreitems.match_id AND winrate_summoner.champion_id = coreitems.champion_id"
# SELECT winrate_summoner.match_id , win_or_lose, champion_lane, winrate_summoner.champion_id, CAST(item_1 AS CHAR(10)), CAST(item_2 AS CHAR(10)), CAST(item_3 AS CHAR(10)), CAST(item_4 AS CHAR(10)) FROM winrate_summoner RIGHT JOIN coreitems ON winrate_summoner.match_id = coreitems.match_id AND winrate_summoner.champion_id = coreitems.champion_id

cur.execute(sql)
rows2 = list(cur.fetchall())
con.commit()
print(type(rows2))
shoes = ["1001", "3006", "3009", "3020", "3047", "3117", "3158", "2422", "3111"]

rows = []

for row in rows2:
    row = list(row)
    if row[4] in shoes:
        print(row)
        row[4] = row[5]
        row[5] = row[6]
        row[6] = row[7]
        del row[7]
        print(row)
    elif row[5] in shoes:
        print(row)
        row[5] = row[6]
        row[6] = row[7]
        del row[7]
        print(row)
    elif row[6] in shoes:
        print(row)
        row[6] = row[7]
        del row[7]
        print(row)
    rows.append(row)




clist = []

roots = []
routes = []
start = Node("start", cnt=0)
roots.append(start)

i = 0
cnt1 = 0
cnt2 = 0
cnt3 = 0
n = 0
try:
    for row in rows:  # 모든 row 보기
        yesitem1 = ""
        yeslane = ""
        yeschamp = ""
        yesitem2 = ""
        yeswinorlose = ""

        if search.find_by_attr(start, row[3], maxlevel=2):  # 중복되는 챔피언 번호가 있다면
            yeschamp = search.find_by_attr(start, row[3], maxlevel=2)
            yeschamp.cnt += 1
            if search.find_by_attr(yeschamp, row[2], maxlevel=2):  # 중복되는 챔피언번호에 중복되는 라인이 있다면
                yeslane = search.find_by_attr(yeschamp, row[2], maxlevel=2)
                yeslane.cnt += 1
                if search.find_by_attr(
                    yeslane, row[1], maxlevel=2
                ):  # 중복되는 챔피언번호에 중복되는 라인에 중복되는 승패가 있다면
                    yeswinorlose = search.find_by_attr(yeslane, row[1], maxlevel=2)
                    yeswinorlose.cnt += 1
                    if search.find_by_attr(yeswinorlose, row[4] + "-1", maxlevel=2):  # 중복되는 챔피언번호에 중복되는 라인에 중복되는 승패에 중복되는 아이템1이 있다면
                        yesitem1 = search.find_by_attr(yeswinorlose, row[4] + "-1", maxlevel=2)
                        yesitem1.cnt += 1
                        if search.find_by_attr(yesitem1, row[5] + "-2", maxlevel=2):
                            yesitem2 = search.find_by_attr(yesitem1, row[5] + "-2", maxlevel=2)
                            yesitem2.cnt += 1
                            if search.find_by_attr(yesitem2, row[6] + "-3", maxlevel=2):
                                yesitem3 = search.find_by_attr(yesitem2, row[6] + "-3", maxlevel=2)
                                yesitem3.cnt += 1
                                i += 1
                                continue
                            else:
                                item_3 = Node(rows[i][6] + "-3", parent=yesitem2, cnt=1)
                                i += 1
                                continue
                        else:
                            item_2 = Node(rows[i][5] + "-2", parent=yesitem1, cnt=1)
                            item_3 = Node(rows[i][6] + "-3", parent=item_2, cnt=1)
                            i += 1
                            continue
                    else:
                        item_1 = Node(rows[i][4] + "-1", parent=yeswinorlose, cnt=1)
                        item_2 = Node(rows[i][5] + "-2", parent=item_1, cnt=1)
                        item_3 = Node(rows[i][6] + "-3", parent=item_2, cnt=1)
                        i += 1
                        continue
                else:
                    win_or_lose = Node(rows[i][1], parent=yeslane, cnt=1)
                    item_1 = Node(rows[i][4] + "-1", parent=win_or_lose, cnt=1)
                    item_2 = Node(rows[i][5] + "-2", parent=item_1, cnt=1)
                    item_3 = Node(rows[i][6] + "-3", parent=item_2, cnt=1)
                    i += 1
                    continue
            else:
                champion_lane = Node(rows[i][2], parent=yeschamp, cnt=1)
                win_or_lose = Node(rows[i][1], parent=champion_lane, cnt=1)
                item_1 = Node(rows[i][4] + "-1", parent=win_or_lose, cnt=1)
                item_2 = Node(rows[i][5] + "-2", parent=item_1, cnt=1)
                item_3 = Node(rows[i][6] + "-3", parent=item_2, cnt=1)
                i += 1
                continue
        else:
            champion_num = Node(rows[i][3], parent=start, cnt=1)
            champion_lane = Node(rows[i][2], parent=champion_num, cnt=1)
            win_or_lose = Node(rows[i][1], parent=champion_lane, cnt=1)
            item_1 = Node(rows[i][4] + "-1", parent=win_or_lose, cnt=1)
            item_2 = Node(rows[i][5] + "-2", parent=item_1, cnt=1)
            item_3 = Node(rows[i][6] + "-3", parent=item_2, cnt=1)
            i += 1
            continue
except:
    print(row)

for pre, fill, node in RenderTree(start):
    tree1 = "%s%s" % (pre, node.name)
    print(tree1.ljust(30), node.cnt)

    tru = node.path[-1]
    path = str(tru).split("'")
    path = path[1]
    itemtree = path

    # 챔피언별 총 경기 수
    if itemtree.count("/") == 3:
        champ_play_num = node.cnt

    if itemtree.count("/") > 6:

        w_or_l = itemtree.split("/")[4]

        # print(itemtree)
        if w_or_l == "win":
            win_cnt = tru.cnt
            lose_cnt = 0
            champ_id = itemtree.split("/")[2]
            champ_lane = itemtree.split("/")[3]
            item_1 = itemtree.split("/")[5]
            item_1 = item_1.replace("-1", "")
            item_2 = itemtree.split("/")[6]
            item_2 = item_2.replace("-2", "")
            item_3 = itemtree.split("/")[7]
            item_3 = item_3.replace("-3", "")
            print(champ_play_num)
            data = {
                "champion_id": champ_id,
                "lane": champ_lane,
                "item_1": item_1,
                "item_2": item_2,
                "item_3": item_3,
                "play_champ_num": champ_play_num,
                "win_cnt": win_cnt,
            }
            df = df.append(data, ignore_index=True)
        elif w_or_l == "lose":
            lose_cnt = tru.cnt
            win_cnt = 0
            champ_id = itemtree.split("/")[2]
            champ_lane = itemtree.split("/")[3]
            item_1 = itemtree.split("/")[5]
            item_1 = item_1.replace("-1", "")
            item_2 = itemtree.split("/")[6]
            item_2 = item_2.replace("-2", "")
            item_3 = itemtree.split("/")[7]
            item_3 = item_3.replace("-3", "")
            print(champ_play_num)
            data2 = {
                "champion_id": champ_id,
                "lane": champ_lane,
                "item_1": item_1,
                "item_2": item_2,
                "item_3": item_3,
                "play_champ_num": champ_play_num,
                "lose_cnt": lose_cnt
            }
            df2 = df2.append(data2, ignore_index=True)

        listlist = []
        listlist.extend([champ_id, champ_lane, item_1, item_2, item_3])
        # print(listlist)
        sql = "INSERT INTO item_rank(index ,champion_id, lane, item_1, item_2, item_3, play_champ_num) values( %s, %s, %s, %s, %s, %s, %s)"

# con.close()
# conn.close()

df3 = pd.merge(df,df2, how='outer',on=('champion_id', 'lane', 'item_1', 'item_2', 'item_3', 'play_champ_num'))

df3 = df3.fillna(0)

def get_win_rate(df):
    if df["win_cnt"] + df["lose_cnt"] == 0:
        return 0
    return round(df["win_cnt"] / (df["win_cnt"] + df["lose_cnt"]), 3)


def get_pick_rate(df):
    return round((df["win_cnt"] + df["lose_cnt"]) / df["play_champ_num"], 3)

df3["win_rate"] = df3.apply(get_win_rate, axis=1)
df3["pick_rate"] = df3.apply(get_pick_rate, axis=1)
#df = df.drop(["win_cnt", "lose_cnt", "play_champ_num"], axis=1)
df3 = df3.reset_index().rename(columns={"index": "index"})

df3.to_csv('./df_testtest3.csv')
df3.to_sql(name="item_rank", con=engine, if_exists="replace", index=False)

con.close()
conn.close()