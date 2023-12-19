import pymysql
import pandas as pd
from tqdm import tqdm
import json
import re

# typ = 1 # ACMFellow: https://awards.acm.org/fellows/award-winners
typ = 10 # A.M. Turing Award: https://amturing.acm.org/byyear.cfm

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


def format_name(name):
    name = name.strip('* .')
    name = re.sub(r"\s*\([^)]*\)", "", name).strip()
    parts = name.split(',')
    if len(parts) == 1:
        return name
    a, b = parts
    c = ''
    for ix, t in enumerate(b.split()):
        if ix >= 1:
            c += t[0] + '. '
        elif len(t) == 1:
            c += t + '. '
        else:
            c += t + ' '
    return c + a
    

#####################################################3
# 1. 读取网页获奖者名单
name2year = {}
if typ == 1:
    with open('out/fellow.txt') as f:
        for line in f.read().strip().split('\n'):
            name = format_name(line.split('ACM Fellows')[0])
            year = int(line.split('ACM Fellows')[-1].strip().split()[0])
            name2year[name] = year
elif typ == 10:
    with open('out/turing.txt') as f:
        lines = f.read().strip().split('\n')
    for i in range(0, len(lines)):
        line = lines[i].strip()
        if line.startswith('(') and line.endswith(')'):
            year = int(line.strip("()"))  # Extracting the year
        else:
            name = format_name(line)
            name2year[name] = year

print('name2year:', name2year)

############################################################
# 2. 读取数据集获奖者
award_df = pd.read_csv('out/award_authors.csv')
award_df = award_df[award_df['type'] == typ]
award_df['MAGID'] = award_df['MAGID'].astype(str).apply(lambda x: x.split('.')[0])
ids = award_df['MAGID'].unique()
print('valid MAGID in award_authors.csv', len(ids))


###########################################################
# 3. （使用网页数据）查询并选出PaperCount最高的3个且大于10的ACM Fellows
id2year = {}
results = []
valid_names = []
for name in tqdm(name2year.keys()):
    try:
        with conn.cursor() as cur:
            query = f"""
                SELECT * FROM MACG.authors
                WHERE name="{name}" AND PaperCount >= 20 AND CitationCount >= 500
                ORDER BY PaperCount DESC;
            """
            # print(query)
            cur.execute(query)
            ret = cur.fetchall()
            results.extend(ret)
            if len(ret) > 0:
                valid_names.append(name)

            for row in ret:
                id2year[str(row[0])] = name2year[name]
    except pymysql.MySQLError as e:
        print(f"Error querying database for {name}")
        print(e)


############################################################
# 4. 添加数据集数据，获取所有作者信息
print('len(valid_names):', len(valid_names))
id2year.update(zip(award_df['MAGID'], award_df['year']))
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
conn.close()

########################################################
# 5. 创建DataFrame并显示结果
df = pd.DataFrame(results, columns=['authorID', 'rank', 'name', 'PaperCount', 'CitationCount'])
df['authorID'] = df['authorID'].astype(str)
df.sort_values(by=['name'], inplace=True, ascending=True)

for name in name2id.keys():
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
df['year'] = df['authorID'].apply(lambda x: id2year[x] if x in id2year else 0)

if typ == 1:
    df.to_csv('out/fellow.csv', index=False)
elif typ == 10:
    df.to_csv('out/turing.csv', index=False)

award_df = pd.DataFrame(columns=['original_author_name', 'year', 'type', 'MAGID', 'ARCID'])
for row in df.iterrows():
    row = row[1]
    if row['authorID'] not in ids:
        award_df.loc[len(award_df)] =[row['name'], row['year'], typ, row['authorID'], 'NULL']

award_df.to_csv(f'out/award_authors_add{typ}.csv', index=False)
