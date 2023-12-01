

mkdir -p out/{papers_raw,papers,links,log}

# python compute_key_papers.py > out/log/compute_key_papers.log
# python update_papers.py > out/log/update_papers.log

# python graph.py
# python compute_similarity_features.py > out/log/compute_similarity_features.log
python run_extract_features.py > out/log/run_extract_features.log
python compute_link_prob.py > out/log/compute_proba.log
python update_links.py > out/log/update_links.log