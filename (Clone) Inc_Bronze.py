# Databricks notebook source
display(dbutils.fs.ls("dbfs:/mnt/cg372/amz-inc-dataset/"))

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/cg372/amz-inc-dataset/")
all_files = []

def list_files_recursive(path):
    items = dbutils.fs.ls(path)
    for item in items:
        if item.isDir():
            list_files_recursive(item.path)
        else:
            all_files.append(item)

list_files_recursive("dbfs:/mnt/cg372/amz-inc-dataset/")
files = all_files
dataframes = {}

for file in files:
    if file.path.endswith(".csv"):
        df_name = file.name.split(".")[0]
        dataframes[df_name] = spark.read.format("csv").option("header", "true").load(file.path)
        print(df_name)
        tablename = df_name.split("_")[0]
        print(tablename)
        # display(dataframes[df_name].limit(1))

# COMMAND ----------

sub_folders = []

def list_sub_folders(path):
    items = dbutils.fs.ls(path)
    for item in items:
        if item.isDir():
            sub_folders.append(item.name)
            list_sub_folders(item.path)

list_sub_folders("dbfs:/mnt/cg372/amz-inc-dataset/")
display(sub_folders)


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog chirag_gajana_databricks_npmentorskool_onmicrosoft_com;
# MAGIC use schema test_ecom;

# COMMAND ----------

for folder in sub_folders:
    path = f"dbfs:/mnt/cg372/amz-inc-dataset/{folder}"
    # print(path)
    files = dbutils.fs.ls(path)
    for file in files:
        folder_name = folder.rstrip('/')
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS chirag_gajana_databricks_npmentorskool_onmicrosoft_com.test_ecom.`{folder_name}`
        """
        spark.sql(create_table_query)
        file_path = f"dbfs:/mnt/cg372/amz-inc-dataset/{folder}{file.name}"
        file_name = file.name.split(".")[0]
        # print(file_name)
        print(file_path)
        if dbutils.fs.ls(file_path):
            copy_into_query = f"""
            COPY INTO {folder_name}
            FROM '{file_path}'
            FILEFORMAT = CSV
            FORMAT_OPTIONS (
                'header'='true',
                'inferSchema'='true',
                'mergeSchema'='true',
                'timestampFormat'='yyyy-MM-dd HH:mm',
                'quote'='"'
            )
            COPY_OPTIONS ('mergeSchema'='true')
            """
            spark.sql(copy_into_query)
        else:
            print(f"File {file_path} does not exist")

# COMMAND ----------

current_version = spark.sql("DESCRIBE HISTORY customers")
display(current_version)
