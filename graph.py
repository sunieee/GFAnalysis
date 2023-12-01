import time
from utils import *

df_paper_reference = pd.read_csv(f"{path}/paper_reference.csv")
df_paper_reference['citingpaperID'] = df_paper_reference['citingpaperID'].astype(str)
df_paper_reference['citedpaperID'] = df_paper_reference['citedpaperID'].astype(str)

# 直接从paper_reference表中筛选出自引的记录
print('creating node & edges', datetime.datetime.now().strftime("%H:%M:%S"))

if not os.path.exists(f'out/edges.csv'):
    print('edges.csv not found, creating self-reference graph...')
    t = time.time()
    # 使用两次 merge 来模拟 SQL 中的 join 操作    
    merged_df1 = df_paper_reference.merge(df_paper_author, left_on='citingpaperID', right_on='paperID')
    merged_df2 = merged_df1.merge(df_paper_author.rename(columns={'authorID': 'authorID2', 'paperID': 'paperID2'}), 
                                    left_on='citedpaperID', right_on='paperID2')
    edges = merged_df2[merged_df2['authorID'] == merged_df2['authorID2']]
    edges = edges[['authorID', 'citingpaperID', 'citedpaperID']]

    print(f'edges created, time cost:', time.time()-t)
    edges.to_csv(f'out/edges.csv', index=False)    
else:   
    edges = pd.read_csv(f'out/edges.csv')
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

with open(f"out/nodes.txt", 'w') as f:
    f.write('\n'.join(nodes))
with open(f"out/paperID_list.txt", 'w') as f:
    f.write('\n'.join(paperID_list))

'''
loading data from database 20:52:47
creating node & edges 20:53:57
edges.csv not found, creating self-reference graph...
edges created, time cost: 58.98010039329529
#nodes: 615328 #edges: 4671822
#paperID_list: 921697
'''