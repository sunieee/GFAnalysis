import pymysql
import pandas as pd
from tqdm import tqdm
import json


# 连接数据库
try:
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db='MACG',
                            charset='utf8')
except pymysql.MySQLError as e:
    print("Error: Unable to connect to the database")
    print(e)

name2year = {}
with open('out/fellow.txt') as f:
    for line in f.read().split('\n'):
        name = line.split('ACM Fellows')[0].strip()
        a, b = name.split(', ')
        c = ''
        for t in b.split():
            if len(t) == 1:
                c += t + '. '
            else:
                c += t + ' '
        name = c + a
        year = int(line.split('ACM Fellows')[-1].strip().split()[0])
        name2year[name] = year
print(name2year)
# save name2year

with open('out/name2year.json', 'w') as f:
    json.dump(name2year, f)


award_df = pd.read_csv('award_authors.csv')
award_df = award_df[award_df['type'] == 1]
ids = [str(x).split('.')[0] for x in award_df['MAGID'].unique()]

# 查询并选出PaperCount最高的3个且大于10的ACM Fellows
results = []
valid_names = []
for name in tqdm(name2year.keys()):
    try:
        with conn.cursor() as cur:
            query = f"""
                SELECT * FROM MACG.authors
                WHERE name="{name}" AND PaperCount > 10 AND CitationCount > 100
                ORDER BY PaperCount DESC
                LIMIT 3;
            """
            # print(query)
            cur.execute(query)
            ret = cur.fetchall()
            results.extend(ret)
            if len(ret) > 0:
                valid_names.append(name)
    except pymysql.MySQLError as e:
        print(f"Error querying database for {name}")
        print(e)

name2id = {}
for id in tqdm(ids):
    try:
        with conn.cursor() as cur:
            query = f"""
                SELECT * FROM MACG.authors
                WHERE authorID="{id}";
            """
            # print(query)
            cur.execute(query)
            ret = cur.fetchall()
            results.extend(ret)
            if len(ret) > 0:
                name2id[ret[0][2]] = id
    except pymysql.MySQLError as e:
        print(f"Error querying database for {id}")
        print(e)

# 关闭数据库连接
conn.close()

# 创建DataFrame并显示结果
# 注意：这里的列名应该与您的数据库表的实际列名相匹配
df = pd.DataFrame(results, columns=['authorID', 'rank', 'name', 'PaperCount', 'CitationCount'])
df.sort_values(by=['name'], inplace=True, ascending=True)

for name in name2id.keys():
    # 删除name相同而id不同的行
    df.drop(df[(df['name'] == name) & (df['authorID'] != name2id[name])].index, inplace=True)

df.drop_duplicates(subset=['authorID'], keep='first', inplace=True)

# name重复，则保留PaperCount最大且CitationCount最大的行，删掉PaperCount和CitationCount不是最大的行
# 定义一个函数来检查每个分组
def filter_group(group):
    # 检查PaperCount和CitationCount是否有记录同时是最大的
    max_paper = group['PaperCount'].max()
    max_citation = group['CitationCount'].max()
    # 如果存在这样的记录，返回这条记录
    if any((group['PaperCount'] == max_paper) & (group['CitationCount'] == max_citation)):
        return group[(group['PaperCount'] == max_paper) & (group['CitationCount'] == max_citation)]
    # 否则返回空DataFrame
    return pd.DataFrame()

df = df.groupby('name').apply(filter_group).reset_index(drop=True)
# df.sort_values(by=['name', 'PaperCount', 'CitationCount'], inplace=True, ascending=False)
# df.drop_duplicates(subset=['name'], keep='first', inplace=True)

df['CitationCount'] = df['CitationCount'].astype(int)
df['PaperCount'] = df['PaperCount'].astype(int)
df['rank'] = df['rank'].astype(int)
df['year'] = df['name'].apply(lambda x: name2year.get(x, 0))
df.to_csv('out/fellow.csv', index=False)

print('len(valid_names):', len(valid_names))