import os
import shutil
import lance
import numpy as np
import pandas as pd
import pyarrow as pa

from LanceDB import LanceDB
from HuggingFaceEncoder import HuggingFaceEncoder

if __name__ == "__main__":

    ## Simple dataframe
    knowledge_base = pd.DataFrame([
        {"id": 1, "text": "LanceDB is an open-source vector database for AI applications, built in Rust.", "category": "database", "date": "2025-08-01"},
        {"id": 2, "text": "It simplifies retrieval, filtering, and management of embeddings.", "category": "database", "date": "2025-08-02"},
        {"id": 3, "text": "DuckDB is an in-process SQL OLAP database management system.", "category": "database", "date": "2025-08-08"},
        {"id": 4, "text": "Python is a high-level, general-purpose programming language.", "category": "language", "date": "2025-08-09"},
        {"id": 5, "text": "Pandas is a popular Python library for data manipulation and analysis.", "category": "language", "date": "2025-08-10"}
    ])
    sample_df = pd.DataFrame({"a": [5]})

    ## Embedding model
    encoder = HuggingFaceEncoder(model_name='sentence-transformers/all-MiniLM-L6-v2')
    EMBEDDING_DIM = encoder.embedding_dim

    knowledge_base['vector'] = list(encoder.encode(knowledge_base['text'].tolist()))
    print("\n--- Generated Knowledge Base DataFrame with Real Embeddings ---")
    print(knowledge_base[['id', 'text', 'category']])
    print("-" * 50)

    ## Lance DB class test 
    root_folder_path = 'db_root_folder'
    knowledge_base_db_name = 'knowledge_base'
    sample_db_name = 'sample'

    sample_lance_db = LanceDB(root_folder_path=root_folder_path, db_name=sample_db_name)
    knowledge_base_db = LanceDB(root_folder_path=root_folder_path, db_name=knowledge_base_db_name)

    knowledge_base_db.write_dataframe_to_lance_format(\
            df=knowledge_base, \
            overwrite=True
        )

    sample_lance_db.write_dataframe_to_parquet_format(\
            df=sample_df
        )
    
    sample_lance_db.convert_parquet_to_lance(\
            overwrite=True
        )
    
    add_df = pd.DataFrame({"a": [50, 100]})
    sample_lance_db.append_df(
            df=add_df
        )

    ## Load specific version
    db_path = os.path.join(root_folder_path, sample_db_name+'.lance')
    dataset = lance.dataset(db_path, version=1)

    ## Check what versions are available and then access specific versions of your dataset
    print('Version:', dataset.versions())

    dataset = dataset.to_table().to_pandas()
    print('sample:', dataset.head())

    ## Indexing
    ### For small datasets, we must reduce the index complexity.
    ### 1. num_partitions=1: With only 5 rows, we only need one cluster.
    ### 2. nbits=2: This lowers the PQ training requirement from 256 (2**8) to 4 (2**2) rows, which is less than our 5 available rows.
    MIN_ROWS_FOR_INDEX = 256 
    if len(knowledge_base) >= MIN_ROWS_FOR_INDEX:
        knowledge_base_db.create_index(
            index_type="IVF_PQ",
            num_partitions=1,
            num_sub_vectors=8,
            nbits=2
        )
    else:
        print(f"Skipping index creation because the number of rows ({len(knowledge_base)}) is less than the minimum required ({MIN_ROWS_FOR_INDEX}).")
        
    ## Filtering & searching test
    print("\n--- Running RAG Pipeline (SQL Date Filter + Vector Search) ---")
    user_question = "What are recent developments in databases?"
    query_vector = encoder.encode([user_question])[0]
    # sql_filter = "date >= '2025-08-05'"
    sql = "SELECT * FROM dataset WHERE date >= '2025-08-05' LIMIT 2"
    print(f"User Question: {user_question}")
    print(f"SQL: {sql}")

    retrieved_data = knowledge_base_db.sql_filter(
        sql=sql
    )
    print(retrieved_data)
    