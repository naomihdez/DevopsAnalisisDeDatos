FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas streamlit plotly sqlalchemy \
    mysql-connector-python openpyxl xlrd pyyaml

COPY . .

RUN mkdir -p data logs data/reports

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
