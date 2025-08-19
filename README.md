Contents
=============

### - Key Features of LanceDB
   * A LanceDB dataset object is not an in-memory copy of your data. Instead, it acts as a lightweight handler that points directly to the data files stored on disk.
   * This design allows LanceDB to handle datasets that are much larger than the available RAM, providing high performance without excessive memory consumption. It intelligently maps and accesses only the data it needs, when it needs it.

### - Lance DB Test
   * Lance DB Class: Build, Convert, Add, Search etc.
   * Lance DB with SQL
   * Simple Lance-DB-Based RAG (to-do)


Docker Environment
=============

### - Docker Build

```
docker build -t lance_db_env .
```

### - Docker Run

```
docker run -it --rm --gpus all --name lance_db --shm-size=64G -p {port}:{port} -e GRANT_SUDO=yes --user root -v {root_folder}:/workspace/lance_db -w /workspace/lance_db lance_db_env bash
```


LanceDB Test
=============
      
```
python test.py
```


Author
=============

#### - LinkedIn: https://www.linkedin.com/in/taeyong-kong-016bb2154

#### - Blog URL: https://blog.naver.com/qbxlvnf11

#### - Email: qbxlvnf11@google.com, qbxlvnf11@naver.com

