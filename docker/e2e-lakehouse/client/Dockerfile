FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.4.4 pyiceberg[pyarrow]==0.10.0 boto3==1.40.61
RUN python -c "import duckdb; c=duckdb.connect(); [c.execute('INSTALL '+e) for e in ['httpfs', 'sqlite', 'ducklake']]"
WORKDIR /app
