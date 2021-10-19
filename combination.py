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
        # "champion_id",
        # "lane",
        "champ_1",
        "champ_2",
        "champ_3",
        # "play_champ_num",
        "win_cnt",
    ]
)

df2 = pd.DataFrame(
    columns=[
        # "champion_id",
        # "lane",
        "champ_1",
        "champ_2",
        "champ_3",
        # "play_champ_num",
        "lose_cnt",
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

sql = "SELECT win_or_lose , id_1, id_2, id_3 FROM TOP_JUG_MID_combination"
# sql = 'SELECT * FROM course LEFT JOIN student ON course.dept = student.dept'
cur.execute(sql)
rows = list(cur.fetchall())
con.commit()


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
        print("1")
        if search.find_by_attr(start, row[0], maxlevel=2):  # 중복되는 챔피언 번호가 있다면
            yeswinorlose = search.find_by_attr(start, row[0], maxlevel=2)
            yeswinorlose.cnt += 1
            if search.find_by_attr(yeswinorlose, row[1], maxlevel=2):  # 중복되는 챔피언번호에 중복되는 라인이 있다면
                yeschamp1 = search.find_by_attr(yeswinorlose, row[1], maxlevel=2)
                yeschamp1.cnt += 1
                if search.find_by_attr(yeschamp1, row[2], maxlevel=2):  # 중복되는 챔피언번호에 중복되는 라인에 중복되는 승패가 있다면
                    yeschamp2 = search.find_by_attr(yeschamp1, row[2], maxlevel=2)
                    yeschamp2.cnt += 1
                    if search.find_by_attr(yeschamp2, row[3], maxlevel=2):  # 중복되는 챔피언번호에 중복되는 라인에 중복되는 승패에 중복되는 아이템1이 있다면
                        yeschamp3 = search.find_by_attr(yeschamp2, row[3], maxlevel=2)
                        yeschamp3.cnt += 1
                        i += 1
                        continue
                    else:
                        champ_3 = Node(rows[i][3], parent=yeschamp2, cnt=1)
                        i += 1
                        continue
                else:
                    champ_2 = Node(rows[i][2], parent=yeschamp1, cnt=1)
                    champ_3 = Node(rows[i][3], parent=champ_2, cnt=1)
                    i += 1
                    continue
            else:
                champ_1 = Node(rows[i][1], parent=yeswinorlose, cnt=1)
                champ_2 = Node(rows[i][2], parent=champ_1, cnt=1)
                champ_3 = Node(rows[i][3], parent=champ_2, cnt=1)
                i += 1
                continue
        else:
            win_or_lose = Node(rows[i][0], parent=start, cnt=1)
            champ_1 = Node(rows[i][1], parent=win_or_lose, cnt=1)
            champ_2 = Node(rows[i][2], parent=champ_1, cnt=1)
            champ_3 = Node(rows[i][3], parent=champ_2, cnt=1)
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
    # if itemtree.count("/") == 3:
    # champ_play_num = node.cnt

    if itemtree.count("/") > 4:

        w_or_l = itemtree.split("/")[2]
        # print(w_or_l)

        # print(itemtree)
        if w_or_l == "win":
            win_cnt = tru.cnt
            # print(win_cnt)
            lose_cnt = 0
            champ_1 = itemtree.split("/")[3]
            champ_2 = itemtree.split("/")[4]
            champ_3 = itemtree.split("/")[5]
            data = {
                "champ_1": champ_1,
                "champ_2": champ_2,
                "champ_3": champ_3,
                "win_cnt": win_cnt,
            }
            df = df.append(data, ignore_index=True)
            # print(df)

        elif w_or_l == "lose":
            lose_cnt = tru.cnt
            # print(win_cnt)
            win_cnt = 0
            champ_1 = itemtree.split("/")[3]
            champ_2 = itemtree.split("/")[4]
            champ_3 = itemtree.split("/")[5]
            data2 = {
                "champ_1": champ_1,
                "champ_2": champ_2,
                "champ_3": champ_3,
                "lose_cnt": lose_cnt,
            }
            df2 = df2.append(data2, ignore_index=True)

df3 = pd.merge(df,df2, how='outer',on=('champ_1', 'champ_2', 'champ_3'))

df3 = df3.fillna(0)

def get_win_rate(df):
    if df["win_cnt"] + df["lose_cnt"] == 0:
        return 0
    return round(df["win_cnt"] / (df["win_cnt"] + df["lose_cnt"]), 3)


def get_pick_rate(df):
    return round((df["win_cnt"] + df["lose_cnt"]) / df["play_champ_num"], 3)

df3["win_rate"] = df3.apply(get_win_rate, axis=1)
#df3["pick_rate"] = df3.apply(get_pick_rate, axis=1)
#df = df.drop(["win_cnt", "lose_cnt", "play_champ_num"], axis=1)
df3 = df3.reset_index().rename(columns={"index": "index"})

#df3.to_csv('./df_testtest3.csv')
df3.to_sql(name="sup_adc_jug_rate", con=engine, if_exists="replace", index=False)

con.close()
conn.close()