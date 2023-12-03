import time
from utils import *
import math
import multiprocessing
from tqdm import tqdm
import json

# 直接从paper_reference表中筛选出自引的记录
print('creating node & edges', datetime.datetime.now().strftime("%H:%M:%S"))
if not os.path.exists(f'out/edges.csv'):
    print('edges.csv not found, creating self-reference graph...')
    t = time.time()
    df_paper_reference = pd.read_csv(f"{path}/paper_reference.csv")
    df_paper_reference['citingpaperID'] = df_paper_reference['citingpaperID'].astype(str)
    df_paper_reference['citedpaperID'] = df_paper_reference['citedpaperID'].astype(str)
    # 使用两次 merge 来模拟 SQL 中的 join 操作    
    merged_df1 = df_paper_reference.merge(df_paper_author, left_on='citingpaperID', right_on='paperID')
    merged_df2 = merged_df1.merge(df_paper_author.rename(columns={'authorID': 'authorID2', 'paperID': 'paperID2'}), 
                                    left_on='citedpaperID', right_on='paperID2')
    edges = merged_df2[merged_df2['authorID'] == merged_df2['authorID2']]
    # 边一定是自引图中的，不用判断一定满足
    # edges = edges[edges['authorID'].isin(authorID_list)]
    edges = edges[['authorID', 'citingpaperID', 'citedpaperID']]
    edges.drop_duplicates(inplace=True)
    edges.to_csv(f'out/edges.csv', index=False)
    df_paper_reference = None
    merged_df1 = None
    merged_df2 = None
    print(f'edges created, time cost:', time.time()-t)
else:   
    edges = pd.read_csv(f'out/edges.csv')
    # edges = edges[edges['citingpaperID'].isin(paperID_list) & edges['citedpaperID'].isin(paperID_list)]
    # edges.drop_duplicates(inplace=True)
    # edges.to_csv(f'out/edges.csv', index=False) 
    edges['authorID'] = edges['authorID'].astype(str)
    edges['citingpaperID'] = edges['citingpaperID'].astype(str)
    edges['citedpaperID'] = edges['citedpaperID'].astype(str)

edges_by_citing = edges.set_index('citingpaperID')
edges_by_cited = edges.set_index('citedpaperID')

nodes = pd.concat([edges['citingpaperID'], edges['citedpaperID']])
nodes = tuple(nodes.drop_duplicates().values)
print('#nodes:', len(nodes), '#edges:', len(edges))

paperID_list = df_paper_author['paperID'].drop_duplicates().tolist()
print('#paperID_list:', len(paperID_list))
# paperID_list是所有节点，而nodes是非孤立点

with open(f"out/nodes.txt", 'w') as f:
    f.write('\n'.join(nodes))
with open(f"out/paperID_list.txt", 'w') as f:
    f.write('\n'.join(paperID_list))
print('nodes & paperID_list saved')

def getYear(pairs):
    paperIDs, info = pairs
    print('extract_paper_years', len(paperIDs), info)
    paperID_str = ','.join([f'\'{x}\'' for x in paperIDs])
    conn, cursor = create_connection()
    # 从数据库中查询
    sql = f"select paperID, year(PublicationDate) as year from MACG.papers where paperID in ({paperID_str})"
    cursor.execute(sql)
    results = cursor.fetchall()
    return dict(results)


if os.path.exists('out/paperID2year.json'):
    paperID2year = json.load(open('out/paperID2year.json'))
else:
    paperID2year = dict(zip(df_papers['paperID'].tolist(), df_papers['PublicationDate'].apply(lambda x: x.year).tolist()))

    citingpaperID_list = [paperID for paperID in df_paper_reference['citingpaperID'].drop_duplicates().tolist() if paperID not in paperID2year]
    print('extracting citingpaper years', len(citingpaperID_list))
    multiproces_num = 20
    group_size = 2000
    group_length = math.ceil(len(citingpaperID_list)/group_size)
    groups = [(citingpaperID_list[i*group_size:i*group_size+group_size], f'{i}/{group_length}') for i in range(group_length)]
    with multiprocessing.Pool(processes=multiproces_num) as pool:
        print('start pool')
        results = pool.map(getYear, groups)
        for result in results:
            paperID2year.update(result)

    with open('out/paperID2year.json', 'w') as f:
        json.dump(paperID2year, f)


if os.path.exists('out/node2citingpaperIDs.json'):
    node2citingpaperIDs = json.load(open('out/node2citingpaperIDs.json'))
else:
    df_paper_reference = pd.read_csv(f"{path}/paper_reference.csv")
    df_paper_reference['citingpaperID'] = df_paper_reference['citingpaperID'].astype(str)
    df_paper_reference['citedpaperID'] = df_paper_reference['citedpaperID'].astype(str)
    #######################################################3
    # 这是关键路径！！优化前： 50h 优化后： 1min
    # 首先，筛选出仅包含在 nodes 中的 citedpaperID
    df_filtered = df_paper_reference[df_paper_reference['citedpaperID'].isin(nodes)]
    # 然后，使用 groupby 创建一个按 'citedpaperID' 分组的字典
    print('grouping by citedpaperID', datetime.datetime.now().strftime("%H:%M:%S"))
    grouped_data = df_filtered.groupby('citedpaperID')['citingpaperID']
    grouped_dict = {}
    for name, group in tqdm(grouped_data):
        grouped_dict[name] = group.tolist()
    # 最后，创建 node 到 citingpaperID 的映射
    node2citingpaperIDs = {node: grouped_dict.get(node, []) for node in nodes}
    df_paper_reference = None
    grouped_data = None
    grouped_dict = None
    with open('out/node2citingpaperIDs.json', 'w') as f:
        json.dump(node2citingpaperIDs, f)


def getTimeseries(paperIDs):
    data = []
    for paperID in tqdm(paperIDs):
        # print(paperID)
        # 不仅包含作者自己的引用，还有其他作者的引用!!!
        citing_years = [paperID2year[paperID] for paperID in node2citingpaperIDs[paperID]]
        if len(citing_years) == 0:
            continue

        # 计算各个年份的被引用次数
        year_counts = pd.Series(citing_years).value_counts().sort_index()
        start_year, end_year = year_counts.index.min(), year_counts.index.max()

        # 初始化一个从起始年份到结束年份的年份列表
        years = list(range(start_year, end_year + 1))
        citation_count_by_year = [year_counts.get(year, 0) for year in years]

        data.append([paperID, start_year, end_year, sum(citation_count_by_year), ','.join(map(str, citation_count_by_year))])
    return data


# key为paperID，value为paperID对应的 [citeStartYear, citeEndYear, totalCitationCount, citationCountByYear]
if not os.path.exists('out/timeseries.csv'):
    print('creating timeseries', datetime.datetime.now().strftime("%H:%M:%S"))
    t = time.time()
    multiproces_num = 20
    with multiprocessing.Pool(processes=multiproces_num) as pool:
        results = pool.map(getTimeseries, [nodes[i::multiproces_num] for i in range(multiproces_num)])
        timeseries = []
        for result in results:
            timeseries += result
    # timeseries = getTimeseries(nodes)
    timeseries_df = pd.DataFrame(timeseries, columns=['paperID', 'citeStartYear', 'citeEndYear', 'totalCitationCount', 'citationCountByYear'])
    timeseries_df.to_csv('out/timeseries.csv', index=False)
    print('timeseries created, time cost:', time.time()-t)


'''
loading data from database 21:06:37
creating node & edges 21:07:47
edges.csv not found, creating self-reference graph...
edges created, time cost: 72.17656636238098
#nodes: 615328 #edges: 2156293
#paperID_list: 921697
'''