import pandas as pd
import time
import pymysql
import os
from tqdm import tqdm
import json
import multiprocessing


database = os.environ.get('database', 'scigene_visualization_field')
def create_connection(database):
    conn = pymysql.connect(host='localhost',
                                user=os.environ.get('user'),
                                password=os.environ.get('password'),
                                db=database,
                                charset='utf8')
    return conn, conn.cursor()

edge_df = pd.read_csv(f'out/{database}/edge_proba.csv')
for col in ['citingpaperID', 'citedpaperID', 'authorID']:
    edge_df[col] = edge_df[col].astype(str)
authorID_list = edge_df['authorID'].unique()

def fetch_citation_context(pairs):
    conn, cursor = create_connection('MACG')
    dic = {}
    for pair in tqdm(pairs):
        citingpaperID, citedpaperID = pair
        query = f"""SELECT citation_context FROM MACG.CitationContextContent
                    where citingpaperID='{citingpaperID}'
                    and citedpaperID='{citedpaperID}'"""
        cursor.execute(query)
        result = cursor.fetchall()
        dic[citingpaperID + ',' + citedpaperID] = '\t'.join([row[0] for row in result])

    conn.close()
    return dic
        

if os.path.exists(f'out/{database}/citation_context.json'):
    print('loading citation context...')
    with open(f'out/{database}/citation_context.json', 'r') as f:
        citation2context = json.load(f)
else:
    print('extracting citation context...')
    t = time.time()

    data = list(zip(edge_df['citingpaperID'], edge_df['citedpaperID']))
    # citation2context = fetch_citation_context(data)
    with multiprocessing.Pool(processes=10) as pool:
        results = pool.map(fetch_citation_context, [data[i::10] for i in range(10)])
    citation2context = {}
    for result in results:
        citation2context.update(result)

    # citingpaperID_list = tuple(edge_df['citingpaperID'].unique())
    # citedpaperID_list = tuple(edge_df['citedpaperID'].unique())
    # citation_context_df = pd.read_sql(f"""SELECT * FROM MACG.CitationContextContent
    #                                 where citingpaperID in {citingpaperID_list}
    #                                 and citedpaperID in {citedpaperID_list}""", conn)
    print('time cost:', time.time()-t)
    with open(f'out/{database}/citation_context.json', 'w') as f:
        json.dump(citation2context, f)

def extract_citation_context(authorID):
    print('##', authorID)
    df = edge_df[edge_df['authorID'] == authorID]

    df = df[['citingpaperID', 'citedpaperID', 'proba']]
    df.columns = ['childrenID', 'parentID', 'extendsProb']
    df['citationContext'] = df.apply(lambda row: citation2context.get(row['childrenID'] + ',' + row['parentID']), axis=1)
    df.to_csv(f'out/{database}/links/{authorID}.csv', index=False)

with multiprocessing.Pool(processes=20) as pool:
    pool.map(extract_citation_context, authorID_list)

"""
当你使用 multiprocessing.Pool 时，每个进程的生命周期确实是函数的生命周期。这意味着每次函数被调用时，都会在一个新的进程上下文中执行。

CPU密集任务：使用多进程

IO密集任务：使用多线程，或者预先创建mysql链接
"""




