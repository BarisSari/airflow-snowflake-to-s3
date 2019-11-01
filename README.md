# airflow-snowflake-to-s3

Virtual Environment might be used to run the project. For creating a proper one, the following commands must be executed respectively:

```bash
pip install virtualenv
virtualenv --python=python3.7 venv
source venv/bin/activate

pip install 'apache-airflow[s3]'
pip install --upgrade snowflake-connector-python
pip install azure-storage-blob==0.37.1
```

