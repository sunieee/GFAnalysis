

mkdir -p out/{papers_raw,papers,links,log}

python extract_fellow.py

python compute_key_papers.py > out/log/compute_key_papers.log
python update_papers.py > out/log/update_papers.log

python graph.py > out/log/graph.log
python compute_similarity_features.py > out/log/compute_similarity_features.log
python extract_link_features.py > out/log/extract_link_features.log
python compute_link_prob.py > out/log/compute_proba.log
python update_links.py > out/log/update_links.log

python analyse_distribution.py > out/log/analyse_distribution.log