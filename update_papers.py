
import pymysql
import pandas as pd
import os
import time
from tqdm import tqdm
import multiprocessing
from collections import defaultdict
import math
import datetime
import json



def create_connection():
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db='MACG',
                            charset='utf8')
    return conn, conn.cursor()

paper_dir = f'out/papers_raw'
file_list = os.listdir(paper_dir)

df_papers = pd.read_csv(f'out/csv/papers.csv')
df_papers['paperID'] = df_papers['paperID'].astype(str)
if 'referenceCount' not in df_papers.columns:
    df_papers['referenceCount'] = -1
top_authors = pd.read_csv(f'out/authors.csv')
top_authors['authorID'] = top_authors['authorID'].astype(str)

authorID_list = list(top_authors['authorID'].unique())
df_paper_author = pd.read_csv(f"out/csv/paper_author.csv")
df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)

paperID_list = df_paper_author['paperID'].drop_duplicates().tolist()
paperID2referenceCount = dict(zip(df_papers['paperID'], df_papers['referenceCount']))
paperID2citationCount = dict(zip(df_papers['paperID'], df_papers['citationCount']))


def extract_paper_abstract(pairs):
    papers, info = pairs
    print('extract_paper_abstract', len(papers), info)
    conn, cursor = create_connection()
    _paperID2abstract = defaultdict(str)

    # 使用IN子句一次查询多个paperID
    # 这个太重要了！！！！！！！ paperID一定要加引号，不然慢1w倍，1s变成10h
    paper_ids_str = ', '.join([f"'{x}'" for x in papers])
    sql = f"""SELECT paperID, abstract FROM abstracts WHERE paperID IN ({paper_ids_str}) ;"""
    # print('*', sql)
    cursor.execute(sql)
    result = cursor.fetchall()

    # 使用Python代码来组合结果
    for paperID, abstract in result:
        _paperID2abstract[paperID] = abstract

    cursor.close()
    conn.close()
    return _paperID2abstract

if os.path.exists(f"out/paperID2abstract.json"):
    with open(f"out/paperID2abstract.json") as f:
        paperID2abstract = json.load(f)
else:
    paperID2abstract = defaultdict(str)
    multiproces_num = 20
    group_size = 1000
    group_length = math.ceil(len(paperID_list)/group_size)
    with multiprocessing.Pool(processes=multiproces_num * 3) as pool:
        results = pool.map(extract_paper_abstract, [(paperID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])
        for result in results:
            paperID2abstract.update(result)
    print('finish extract_paper_abstract', len(paperID2abstract))
    with open(f"out/paperID2abstract.json", 'w') as f:
        json.dump(paperID2abstract, f)


def extract_paper_authors(pairs):
    papers, info = pairs
    print('extract_paper_authors', len(papers), info)
    conn, cursor = create_connection()
    _paperID2authorsName = defaultdict(list)

    # 使用IN子句一次查询多个paperID
    paper_ids_str = ', '.join([f"'{x}'" for x in papers])
    cursor.execute(f"""SELECT paper_author.paperID, authors.name
                       FROM paper_author 
                       JOIN authors ON paper_author.authorID=authors.authorID 
                       WHERE paper_author.paperID IN ({paper_ids_str})
                       ORDER BY paper_author.paperID, paper_author.authorOrder;""")
    result = cursor.fetchall()

    # 使用Python代码来组合结果
    for paperID, name in result:
        _paperID2authorsName[paperID].append(name)
    for paperID, names in _paperID2authorsName.items():
        _paperID2authorsName[paperID] = ', '.join(names)
    conn.close()
    return _paperID2authorsName


def valid_venue(venu):
    if venu is None:
        return False
    if venu in ['None', ' ', '', '0']:
        return False
    return True

def extract_paper_venu(papers):
    conn, cursor = create_connection()
    _paperID2venue = {}
    for paperID in tqdm(papers):
        cursor.execute(f"select ConferenceID, JournalID from papers where paperID='{paperID}'")
        result = cursor.fetchone()
        # print(result)
        venu = None
        if valid_venue(result[0]):
            cursor.execute("select abbreviation, name from MACG.conferences where conferenceID=%s", (result[0],))
            res = cursor.fetchone()
            if valid_venue(res):
                venu = res[1] + ' (' + res[0] + ')'
        elif valid_venue(result[1]):
            cursor.execute("select name from MACG.journals where journalID=%s", (result[1],))
            res = cursor.fetchone()
            if res != None:
                venu = res[0]
        _paperID2venue[paperID] = venu

    conn.close()
    return _paperID2venue

paperID2venue = defaultdict(str)
paperID2authorsName = defaultdict(str)

paperID_list = []
for file in tqdm(file_list):
    filepath = os.path.join(paper_dir, file)
    papers = pd.read_csv(filepath)
    papers['paperID'] = papers['paperID'].astype(str)
    paperID_list += papers["paperID"].values.tolist()
paperID_list = list(set(paperID_list))
print('len(paperID_list)', len(paperID_list), datetime.datetime.now().strftime('%H:%M:%S'))

multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_venu, [paperID_list[i::multiproces_num] for i in range(multiproces_num)])
    for result in results:
        paperID2venue.update(result)
print('finish extract_paper_venu', len(paperID2venue), datetime.datetime.now().strftime('%H:%M:%S'))

group_size = 2000
group_length = math.ceil(len(paperID_list)/group_size)
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_authors, [(paperID_list[i*group_size:i*group_size+group_size], f'{i}/{group_length}') for i in range(group_length)])
    for result in results:
        paperID2authorsName.update(result)
print('finish extract_paper_authors', len(paperID2authorsName), datetime.datetime.now().strftime('%H:%M:%S'))

def extract_paper(file):
    filepath = os.path.join(paper_dir, file)
    papers = pd.read_csv(filepath)
    for col in ["authorOrder", "firstAuthorID", "firstAuthorName"]:
        try:
            papers = papers.drop(columns=[col])
        except:
            pass
    papers['paperID'] = papers['paperID'].astype(str)
    papers['referenceCount'] = papers['paperID'].apply(lambda paperID: paperID2referenceCount[paperID])
    papers['citationCount'] = papers['paperID'].apply(lambda paperID: paperID2citationCount[paperID])
    papers['venu'] = papers['paperID'].apply(lambda paperID: paperID2venue[paperID])
    papers['authorsName'] = papers['paperID'].apply(lambda paperID: paperID2authorsName[paperID])
    papers['abstract'] = papers['paperID'].apply(lambda paperID: paperID2abstract.get(paperID, ''))

    papers.to_csv(f'out/papers/' + filepath.split('/')[-1], index=False)

    core_papers = papers[papers['isKeyPaper'] > 0.5]
    core_citations = core_papers['citationCount'].to_list()
    core_citations.sort(reverse=True)

    citations = papers['citationCount'].to_list()
    citations.sort(reverse=True)
    return {
        'authorID': file.split('.')[0],
        'hIndex': sum(1 for i, citation in enumerate(citations) if citation > i),
        'CorePaperCount': len(core_papers),
        'CoreCitationCount': core_papers['citationCount'].sum(),
        'CorehIndex': sum(1 for i, citation in enumerate(core_citations) if citation > i)
    }

multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper, file_list)

df = pd.DataFrame(results)
# remove columns in top_authors if exist: ['CorePaperCount', 'CoreCitationCount', 'CorehIndex']
for col in ['CorePaperCount', 'CoreCitationCount', 'CorehIndex']:
    if col in top_authors.columns:
        top_authors = top_authors.drop(columns=[col])
top_authors = top_authors.merge(df, on='authorID')

top_authors.to_csv(f'out/top_authors.csv', index=False)