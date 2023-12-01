import csv
import re
import pandas as pd
import gensim
import nltk
from nltk.corpus import stopwords
import ssl
import numpy as np
from gensim import utils
import json
import os
import time
from tqdm import tqdm
import multiprocessing

# run before
# nltk.download('stopwords')
x_train = []

with open(f'out/nodes.txt', 'r') as f:
    nodes = f.read().split()

with open(f'out/paperID_list.txt', 'r') as f:
    paperID_list = f.read().split()


############################################################
# 实际上，你只需要训练Doc2Vec模型一次。
# 一旦模型被训练，你可以多次使用它来推断新的文档向量，而不需要重新训练。
# RETRAIN_GENSIM = False 时，加载已经训练好的模型，并使用它来推断新的文档向量
############################################################
# read all the selected papers in MAG
with open(f"out/paperID2abstract.json") as f:
    paperID2abstract = json.load(f)

db = pd.read_csv(f'out/csv/papers.csv')
db['paperID'] = db['paperID'].astype(str)
db = db[db['paperID'].isin(nodes)]
db['abstract'] = db['paperID'].map(paperID2abstract)
db = db[['paperID', 'title', 'abstract']]
db.reset_index(drop=True, inplace=True)

print('similarity_mysql:',len(db))
print(db.head())

# Drop the first column and convert to list
result = db.iloc[:, 1:].values.tolist()

# Append the first value of each row twice
result = [row + [row[0], row[0]] for row in result]

# print(result[:5])

# Tokenize and remove stopwords
stoplist = set(stopwords.words('english'))
def tokenize(text):
    return [word for word in utils.tokenize(text.lower()) if word not in stoplist]

result_token = []
for row in tqdm(result):
    result_token.append(tokenize(' '.join([str(item) if item else '' for item in row]).lower()))

# Create TaggedDocument for training
x_train = [gensim.models.doc2vec.TaggedDocument(words=row, tags=[i]) for i, row in enumerate(result_token)]

# Train the Doc2Vec model
if os.path.exists(f"out/model1.txt"):
    # Load the trained model
    model = gensim.models.doc2vec.Doc2Vec.load(f"out/model1.txt")
else:
    print('start training gensim')
    t = time.time()
    model = gensim.models.doc2vec.Doc2Vec(vector_size=2, min_count=5, epochs=20)
    model.build_vocab(x_train)
    model.train(x_train, total_examples=model.corpus_count, epochs=model.epochs)
    model.save(f"out/model1.txt")
    print('finish training gensim, time cost:', time.time()-t)

# Infer vectors for each document
def infer_vector(doc):
    return model.infer_vector(doc.words)

# 定义一个处理函数
with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
    output = pool.map(infer_vector, tqdm(x_train))

# output = []
# for doc in tqdm(x_train):
#     output.append(model.infer_vector(doc.words))


# Create a DataFrame with the features
df_feature = pd.DataFrame(output)
df_feature = pd.concat([db['paperID'], df_feature], axis=1)

# Drop duplicates and rows with 'paperID' as 'paperID'
df_feature = df_feature.drop_duplicates()
df_feature = df_feature[df_feature['paperID'] != 'paperID']

# Save to CSV
df_feature.to_csv(f'out/similarity_features.csv', index=False)
print('similarity_features saved', len(df_feature))
print(df_feature.head())
