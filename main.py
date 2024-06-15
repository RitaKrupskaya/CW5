import psycopg2

from src.config import config
from src.get_from_hh import get_companies, get_vacancies, create_database, save_to_database
from src.db_manager import DBManager


def main():
    employers_data = get_companies()
    vacancies_data = get_vacancies(employers_data)
    params = config()

    create_database('CW5', params)

    conn = psycopg2.connect(dbname='CW5', **params)
    save_to_database(employers_data, vacancies_data, 'CW5', params)
    conn.close()

    conn = psycopg2.connect(dbname='CW5', **params)
    database_manager = DBManager(conn)

    """
    Начинаем взаимодействие с пользователем
    """
    print("""Введите нужное число для получения результата:
          1 - Список всех компаний и количество вакансий у каждой компании
          2 - Список всех вакансий, в названии которых содержатся ключевые слова         
          3 - Средняя зарплата по вакансиям
          4 - Список всех вакансий, у которых зарплата выше средней по всем вакансиям
          5 - Cписок всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию """)

    user_input = input()
    if user_input == '1':
        companies_and_vacancies_count = database_manager.get_companies_and_vacancies_count()
        print("Компании и количество доступных вакансий:")
        for company_name, vacancy_counter in companies_and_vacancies_count:
            print(f"{company_name}: {vacancy_counter}")

    elif user_input == '2':
        keyword = input("Введите ключевое слово: ")
        vacancies_with_keyword = database_manager.get_vacancies_with_keyword(keyword)
        print(f"Все вакансии с ключевым словом '{keyword}':")
        for vacancy in vacancies_with_keyword:
            employer_name, vacancy_name, salary_from, salary_to, vacancy_url = vacancy
            print(f"Компания: {employer_name}, Вакансия: {vacancy_name}, Зарплата: {salary_from}-{salary_to},"
                  f"Ссылка на вакансию: {vacancy_url}")

    elif user_input == '3':
        avg_salary = database_manager.get_avg_salary()
        print(f"Средняя зарплата по всем вакансиям: {avg_salary}")

    elif user_input == '4':
        higher_salary_vacancies = database_manager.get_vacancies_with_higher_salary()
        print("Вакансии с зарплатой выше средней:")
        for vacancy in higher_salary_vacancies:
            employer_name, vacancy_name, salary_from, salary_to, vacancy_url = vacancy
            print(f"Компания: {employer_name}, Вакансия: {vacancy_name}, Зарплата: {salary_from}-{salary_to},"
                  f"Ссылка на вакансию: {vacancy_url}")

    elif user_input == '5':
        all_vacancies = database_manager.get_all_vacancies()
        print("Все вакансии:")
        for vacancy in all_vacancies:
            employer_name, vacancy_name, salary_from, salary_to, vacancy_url = vacancy
            print(f"Компания: {employer_name}, Вакансия: {vacancy_name}, Зарплата: {salary_from}-{salary_to}, "
                  f"Ссылка на вакансию: {vacancy_url}")
    else:
        print("Ошибка ввода")

    database_manager.__del__()
    conn.close()


if __name__ == '__main__':
    main()
