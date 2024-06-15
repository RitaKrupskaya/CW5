from config import config
import psycopg2


class DBManager:
    """
    Класс для подключения к базе данных PostgreSQL
    """

    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        """
        self.cursor.execute("""
            SELECT c.employer_name, COUNT(v.vacancy_id) AS vacancy_counter 
            FROM companies c
            LEFT JOIN vacancies v USING(employer_id)
            GROUP BY c.employer_name;
        """)
        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий,
        в названии которых содержатся переданные в метод слова, например python.
        """
        query = """
                    SELECT * FROM vacancies
                    WHERE LOWER(vacancy_name) LIKE %s
                    """
        self.cursor.execute(query, ('%' + keyword.lower() + '%',))
        return self.cursor.fetchall()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям
        """
        self.cursor.execute("""
            SELECT ROUND(AVG((salary_from + salary_to) / 2)) AS avg_salary
            FROM vacancies;
        """)
        return self.cursor.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        """
        self.cursor.execute("""
            SELECT * FROM vacancies
            WHERE (salary_from + salary_to) > 
            (SELECT AVG(salary_from + salary_to) FROM vacancies);
        """)
        return self.cursor.fetchall()

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию
        """
        self.cursor.execute("""
            SELECT c.employer_name, v.vacancy_name, v.salary_from, v.salary_to, v.vacancy_url
            FROM companies c
            JOIN vacancies v USING(employer_id);
        """)
        return self.cursor.fetchall()

    def __del__(self):
        self.cursor.close()
