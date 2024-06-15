import requests
import psycopg2
import json
from typing import Any


def get_companies():
    """
    Получает имя компаний и их ID top_employers.json
    """
    with open('top_employers.json', 'r', encoding='utf-8') as f:
        employers_data = json.load(f)[0]

    data = []

    for employer_name, employer_id in employers_data.items():
        employer_url = f"https://hh.ru/employer/{employer_id}"
        employer_info = {'employer_id': employer_id, 'employer_name': employer_name, 'employer_url': employer_url}
        data.append(employer_info)

    return data


def get_vacancies(data):
    """
    Получает информацию о вакансиях для компаний из списка data

    """
    vacancies_info = []
    for employer_data in data:
        employer_id = employer_data['employer_id']
        url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
        response = requests.get(url)
        if response.status_code == 200:
            vacancies = response.json()['items']
            vacancies_info.extend(vacancies)
        else:
            print(f"Ошибка при запросе к API: {response.status_code}")
    return vacancies_info


def create_database(database_name, params):
    """
    Создание базы данных для сохранения данных о компаниях и вакансиях
    """
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(f"DROP DATABASE {database_name}")
    except Exception as e:
        print(f"Ошибка создания базы данных: {e}")
    finally:
        cur.execute(f"CREATE DATABASE {database_name}")

    cur.close()
    conn.close()

    """
    Создание таблицы с выбранными компаниями с такими параметрами как:
    - id компании
    - название компании
    - ссылка на страницу компании на hh.ru
    """
    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE companies (
            employer_id integer PRIMARY KEY,
            employer_name varchar(50) NOT NULL,
            employer_url text
            )
        """)

    """
    Создание таблицы с вакансиями с такими параметрами как:
    - id вакансии
    - id компании
    - название вакансии 
    - минимальная и макисмальная заработная плата 
    - ссылка на вакансию
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
            vacancy_id text PRIMARY KEY,
            employer_id integer REFERENCES companies(employer_id),
            vacancy_name varchar(100) NOT NULL,
            salary_from integer,
            salary_to integer,
            vacancy_url text
            )
        """)

    conn.commit()
    conn.close()


def save_to_database(employers_data, vacancies_data, database_name, params):
    """
     Добавление данных о компаниях и вакансиях в базу данных
     """
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in employers_data:
            employer_id = employer['employer_id']
            employer_name = employer['employer_name']
            employer_url = employer['employer_url']
            cur.execute("""
                INSERT INTO companies (employer_id, employer_name, employer_url)
                VALUES (%s, %s, %s)
            """, (employer_id, employer_name, employer_url))

        for vacancy in vacancies_data:
            vacancy_id = vacancy['id']
            employer_id = vacancy['employer']['id']
            vacancy_name = vacancy['name']
            salary = vacancy['salary']
            salary_from = salary.get('from') if salary else None
            salary_to = salary.get('to') if salary else None
            vacancy_url = vacancy['alternate_url']
            cur.execute("""
                INSERT INTO vacancies (vacancy_id, employer_id, vacancy_name, salary_from, salary_to, vacancy_url)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (vacancy_id, employer_id, vacancy_name, salary_from, salary_to, vacancy_url))

    conn.commit()
    conn.close()
