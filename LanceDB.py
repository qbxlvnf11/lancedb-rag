import os
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa

import lance
from lance.vector import vec_to_table
import lancedb
from lancedb.index import FTS
import duckdb

from utils import parsing_df

class LanceDB:

    # db = None
    dataset = None
    root_folder_path = ''
    db_name = ''

    def __init__(self, \
            root_folder_path: str = '', \
            db_name: str = ''
        ):

        self.root_folder_path = root_folder_path
        self.db_name = db_name
        self.lance_db_path = os.path.join(self.root_folder_path, self.db_name+'.lance')
        self.parquet_path = os.path.join(self.root_folder_path, self.db_name+'.parquet')
        print('Lance DB path:', self.lance_db_path)
        
        # shutil.rmtree(self.lance_db_path, ignore_errors=True)
        # shutil.rmtree(self.parquet_path, ignore_errors=True)

        self.has_index = False

        # if os.path.exists(self.lance_db_path):
            # self.load()
            # self.__connect_db()

    # def load(self):

    #     print(f"Loading dataset from: {self.lance_db_path}")
    #     self.dataset = lance.dataset(self.lance_db_path)
        
    #     if self.dataset.list_indices():
    #         self.has_index = True

    # def __connect_db(self):
        
    #     print(f"Connecting dataset from: {self.lance_db_path}")
    #     self.db = lancedb.connect(self.lance_db_path)

    #     self.db.create_table(
    #         name="connect_db",
    #         data=self.lance_db_path
    #     )

    #     print('self.db.table_names():', self.db.table_names())
    #     for table_name in self.db.table_names():
    #         print(f'- Table name: {table_name}')

    ## Write dataframe to Lance
    def write_dataframe_to_lance_format(self, \
            df: pd.DataFrame = pd.DataFrame({"test": [0]}), \
            overwrite: bool = False
        ):

        mode = 'create'
        if overwrite:
            mode = 'overwrite'
        
        tbl = parsing_df(df)

        self.dataset = lance.write_dataset(tbl, self.lance_db_path, mode=mode)
        self.has_index = False
        print(f"Writing dataframe to: {self.lance_db_path}")

        ## Check what versions are available and then access specific versions of your dataset
        # print('Version:', self.dataset.versions())

        test_pd = self.dataset.to_table().to_pandas()
        print('write_dataframe_to_lance_format sample:', test_pd.head())

    ## Write dataframe to Parquet
    def write_dataframe_to_parquet_format(self, \
            df: pd.DataFrame = pd.DataFrame({"test": [0]}), \
        ):

        if os.path.exists(self.parquet_path):
            print(f'{self.parquet_path} already exists!')
            return None

        tbl = pa.Table.from_pandas(df)
        pa.dataset.write_dataset(tbl, self.parquet_path, format='parquet')
        print(f"Writing dataframe to: {self.parquet_path}")

        parquet = pa.dataset.dataset(self.parquet_path)
        test_pd = parquet.to_table().to_pandas()
        print('write_dataframe_to_parquet_format sample:', test_pd.head())

    ## Convert Parquet to Lance
    def convert_parquet_to_lance(self, \
            overwrite: bool = False
        ):

        parquet = pa.dataset.dataset(self.parquet_path)

        mode = 'create'
        if overwrite:
            mode = 'overwrite'

        elif os.path.exists(self.lance_db_path):
            print(f'{self.lance_db_path} already exists!')
            return None
        
        self.dataset = lance.write_dataset(parquet, self.lance_db_path, mode=mode)
        self.has_index = False
        print(f"Converting to: {self.lance_db_path}")

        ## Check what versions are available and then access specific versions of your dataset
        # print('Version:', self.dataset.versions())

        test_pd = self.dataset.to_table().to_pandas()
        print('convert_parquet_to_lance sample:', test_pd.head())

    ## Append Parquet to Lance
    def append_df(self, \
            df: pd.DataFrame = pd.DataFrame({"test": [0]}), \
        ):
        
        tbl = pa.Table.from_pandas(df)
        
        self.dataset = lance.write_dataset(tbl, self.lance_db_path, mode="append")

        test_pd = self.dataset.to_table().to_pandas()
        print('append_df sample:', test_pd.head())

    ## Indexing
    def create_index(self, \
            index_type: str = 'IVF_PQ', \
            num_partitions: int = 2, \
            num_sub_vectors: int = 8, \
            nbits: int = 8
        ):

        if self.dataset is None:
            raise ValueError("Dataset not loaded.")
        
        # self.load()
        dataset = lance.dataset(self.lance_db_path)
        assert isinstance(dataset, pa.dataset.Dataset)

        print(f"\n--- Creating {index_type} Index ---")
        self.dataset.create_index(
            "vector", \
            index_type=index_type, \
            num_partitions=num_partitions, \
            num_sub_vectors=num_sub_vectors, \
            nbits=nbits
        )
        self.has_index = True
        print("Index created successfully.")
        print("-" * 50)

    ## Filtering & Searching
    def sql_filter(self, \
        query_vector: np.array = '', \
        sql: str = '', 
        # limit: int = 2, \
        # nprobes: int = 10
    ):
        
        if self.dataset is None:
            raise ValueError("Dataset not loaded.")
        
        # self.load()
        self.dataset = lance.dataset(self.lance_db_path)
        assert isinstance(self.dataset, pa.dataset.Dataset)

        conn = duckdb.connect()
        conn.register('dataset', self.dataset)

        return conn.execute(sql).df()

        # search_query = self.dataset.search(query_vector)

        # print("\n--- Retrieved Data ---")
        # if self.has_index:
        #     print(f"Applying search refinement with nprobes = {nprobes}")
        #     ## How many data groups (clusters) will you explore?
        #     search_query = search_query.nprobes(nprobes)

        # else:
        #     print("No index found. Performing brute-force search (nprobes will be ignored).")
            
        # if sql_filter:
        #     search_query = search_query.where(sql_filter)

        # retrieved_data = search_query.limit(limit).to_df()

        # return retrieved_data

