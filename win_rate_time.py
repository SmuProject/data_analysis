import pymysql
import pandas as pd
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()

def win_rate_time(ft, st):
    print("DATABASE 연결 중")
    con = pymysql.connect(host='tae2089.synology.me',
                          port=51420,
                          user='test',
                          password='test',
                          db='test')
    print("DATABASE 연결 완료")

    # 디폴트 커서 생성
    cur = con.cursor()
    fts = ft*60
    if not st:
        st = 'end'
        sql = "SELECT match_id, my_champ, win, game_duration from test.overall where game_duration >= " + str(fts)

    else:
        sts = st*60
        sql = "SELECT match_id, my_champ, win, game_duration from test.overall where game_duration < " + str(
            sts) + " and game_duration >= " + str(fts)

    cur.execute(sql)
    rows = list(cur.fetchall())
    con.commit()

    col_name = ['match_id', 'my_champ', 'win', 'game_duration']
    list_df = pd.DataFrame(rows, columns=col_name)
    list_df = list_df.drop(['game_duration', 'match_id'], axis=1)
    count1 = list_df.groupby([list_df['my_champ'], list_df['win']]).size()
    count2 = pd.DataFrame(count1)

    i = 1
    little = []
    win = []
    lose = []
    for i in range(1, 900, 1):
        try:
            first = count2[0][i][0]
        except:
            first = 0

        try:
            second = count2[0][i][1]
        except:
            second = 0

        if first == 0:
            if second == 0:
                continue

        end = first + second

        little.append(end)
        win.append(second)
        lose.append(first)
        i += 1

    df = pd.DataFrame(columns=['champ_id', 'win+lose', 'win', 'lose'])
    nu = []
    it = len(count2.index)
    for i in range(it):
        if count2.index[i][0] not in nu:
            nu.append(count2.index[i][0])

    df['champ_id'] = nu
    df['win+lose'] = little
    df['win'] = win
    df['lose'] = lose

    df['winrate'] = round(df['win'] / (df['win'] + df['lose']), 3)
    print(df)

    engine = create_engine("mysql+mysqldb://test:" + "test" + "@tae2089.synology.me:51420/test", encoding='utf-8')
    conn = engine.connect()

    df.to_sql(name='champ_rank_'+str(ft)+'to'+str(st), con=engine, if_exists='replace', index=False)

    con.close()
    conn.close()

win_rate_time(10,20)
win_rate_time(20,30)
win_rate_time(30,40)
win_rate_time(40,None)