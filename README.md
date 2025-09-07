Contents
=============

### - Key Features of [LanceDB](https://lancedb.com/) ([Git](https://github.com/lancedb/lancedb))
   * A LanceDB dataset object is not an in-memory copy of your data. Instead, it acts as a lightweight handler that points directly to the data files stored on disk.
   * This design allows LanceDB to handle datasets that are much larger than the available RAM, providing high performance without excessive memory consumption. It intelligently maps and accesses only the data it needs, when it needs it.

### - Lance DB RAG Test
   * Lance DB Class: Build, Convert, Add, Search etc.
   * Simple Lance-DB-Based RAG (Encoder: [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2))
   * Lance DB SQL
   * Lance DB Vector Searching
   * Lance DB Vector Searching with Metadata Filtering (SQL)


Docker Environment
=============

### - Docker Build

```
docker build -t lance_db .
```

### - Docker Run

```
docker run -it --rm --gpus all --name lance_db_env --shm-size=64G -p {port}:{port} -e GRANT_SUDO=yes --user root -v {root_folder}:/workspace/lance_db -w /workspace/lance_db lance_db bash
```


LanceDB RAG Test
=============
      
```
python test.py
```


Author
=============

#### - [LinkedIn](https://www.linkedin.com/in/taeyong-kong-016bb2154)

#### - [Blog URL](https://blog.naver.com/qbxlvnf11)

#### - Email: qbxlvnf11@google.com, qbxlvnf11@naver.com

