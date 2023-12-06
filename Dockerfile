FROM python:3.11

RUN mkdir /code

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

#RUN pip install pdm fastapi uvicorn streamlit prefect httpx python-dotenv pandas duckdb==0.8.1

COPY . /code

RUN pip install pdm

#RUN pdm config python.use_venv false && pdm install
RUN pdm install && python .venv/bin/activate_this.py

EXPOSE 8000