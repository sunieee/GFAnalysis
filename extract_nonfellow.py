import pandas as pd
from tqdm import tqdm

fellow_df = pd.read_csv('out/fellow.csv')

candidate_databases = [
    'scigene_database_field',
    'scigene_VCG_field'
    # 'scigene_AI_field'
]
candidates = pd.concat([pd.read_csv(f'../compute_prob/out/{database}/top_field_authors.csv') for database in candidate_databases])
candidates.drop(columns=[col for col in candidates.columns if col.endswith('_field')], inplace=True)


# replace '\N' with 0
candidates['PaperCount'] = candidates['PaperCount'].replace(r'\N', 0).astype(int)
candidates['CitationCount'] = candidates['CitationCount'].replace(r'\N', 0).astype(int)


fellow_authors = set(fellow_df['authorID'].to_list())

non_fellow_df = []
ratio = 0.05
for row in tqdm(fellow_df.to_dict(orient='records')):
    condition1 = (1 - ratio < candidates['PaperCount'] / row['PaperCount']) & (candidates['PaperCount'] / row['PaperCount'] < 1 + ratio)
    condition2 = (1 - ratio < candidates['CitationCount'] / row['CitationCount']) & (candidates['CitationCount'] / row['CitationCount'] < 1 + ratio)
    nonfellow_candidates = candidates[condition1 & condition2].copy()
    nonfellow_candidates = nonfellow_candidates[~nonfellow_candidates['authorID'].isin(fellow_authors)]
    nonfellow_candidates['compareAuthorID'] = row['authorID']
    nonfellow_candidates['deviation'] = abs(nonfellow_candidates['PaperCount'] / row['PaperCount'] - 1) + abs(nonfellow_candidates['CitationCount'] / row['CitationCount'] - 1)
    nonfellow_candidates = nonfellow_candidates.sort_values(by='deviation', ascending=True).head(5)
    non_fellow_df.append(nonfellow_candidates)
    
non_fellow_df = pd.concat(non_fellow_df)
non_fellow_df.to_csv('non_fellow.csv', index=False)

fellow_df['fellow'] = True
non_fellow_df['fellow'] = False
author_df = pd.concat([fellow_df, non_fellow_df])
author_df.fillna(0, inplace=True)
author_df['year'] = author_df['year'].astype(int)
author_df['compareAuthorID'] = author_df['compareAuthorID'].astype(int)

author_df.to_csv('out/authors.csv', index=False)