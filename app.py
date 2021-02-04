from flask import Flask, render_template, request
from config import config
import snowflake.connector as sf
from snowflake.connector import DictCursor
from pathlib import Path
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

conn = sf.connect(user=config.username, password=config.password, database=config.database, warehouse=config.warehouse,
                  account=config.account, role=config.role, schema=config.schema)

sem = threading.Semaphore()
app = Flask(__name__)


def keep_alive():
    global conn
    try:
        cursor = conn.cursor(DictCursor)
        execute_query(cursor, get_number_of_rows())
    except:
        conn = sf.connect(user=config.username, password=config.password, database=config.database,
                          warehouse=config.warehouse,
                          account=config.account, role=config.role, schema=config.schema)


scheduler = BackgroundScheduler()
scheduler.add_job(func=keep_alive, trigger="interval", hours=1)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())


def execute_query(cursor, query):
    cursor.execute(query)
    result = list(cursor)
    if not len(result):
        return None
    else:
        cursor_dict = result[0]
        return next(iter(cursor_dict.values()))


def read_counter():
    with open('counter.txt', 'r') as f:
        counter = f.readline()
    return int(counter)


def write_counter(num):
    with open('counter.txt', 'w') as f:
        f.write('%d' % num)


def get_next_available(counter):
    sql = f"""SELECT MIN(t.identifier) + 1 as NextId
            FROM uniqueidentifier t
            LEFT OUTER JOIN uniqueidentifier t2 on t.identifier+1=t2.identifier
            WHERE t.identifier >= {counter} and t2.identifier is null"""
    return sql


def reclaim_resources(counter):
    sql = f"""
    DELETE FROM uniqueidentifier
    WHERE identifier <= {counter};
    """
    return sql


def check_if_seen(counter):
    sql = f"""
    SELECT 1
    FROM uniqueidentifier
    WHERE identifier = {counter};
    """
    return sql


def reset_database():
    sql = """
     DELETE FROM uniqueidentifier;
    """
    return sql


def get_number_of_rows():
    sql = """
    SELECT COUNT(identifier) as rowcount
    FROM uniqueidentifier;
    """
    return sql


@app.route('/')
def main():
    my_file = Path("counter.txt")
    if not my_file.is_file():
        write_counter(1)
    return render_template('app.html')


def add_identifier(param):
    sql = f"""INSERT INTO UNIQUEIDENTIFIER VALUES ({param});"""
    return sql


def is_valid(user_input):
    if user_input == "":
        return True
    try:
        int(user_input)
    except ValueError:
        return False
    return 0 < int(user_input) < pow(10, 38)


@app.route('/send', methods=['POST'])
def send():
    if request.method == "POST":
        if not is_valid(request.form['num1']):
            return render_template('app.html', sum="When requesting a specific number, please input a number "
                                                   "between 0 and 10^38 (non-inclusive).")
        sem.acquire(timeout=10)
        cursor = conn.cursor(DictCursor)
        given_num = "" if request.form['num1'] == "" else int(request.form['num1'])
        counter = read_counter()
        message = "Sorry, something went wrong."
        size_message = ""
        try:
            if given_num == "" or given_num <= counter or execute_query(cursor, check_if_seen(given_num)):
                execute_query(cursor, add_identifier(counter))
                result = execute_query(cursor, get_next_available(counter))
                write_counter(result)
                execute_query(cursor, reclaim_resources(result))
                size = execute_query(cursor, get_number_of_rows())
                message = f"Your next identifier is: {counter}"
                size_message = f"(# Rows in Snowflake = {size})"
            else:
                execute_query(cursor, add_identifier(given_num))
                size = execute_query(cursor, get_number_of_rows())
                message = f"Your next identifier is: {given_num}"
                size_message = f"(# Rows in Snowflake = {size})"
        finally:
            cursor.close()
            sem.release()
            return render_template('app.html', sum=message, size=size_message)


@app.route('/reset', methods=['POST'])
def reset():
    if request.method == "POST":
        write_counter(1)
        cursor = conn.cursor(DictCursor)
        message = "Something went wrong."
        try:
            execute_query(cursor, reset_database())
            message = f"You've resetted the database."
        finally:
            cursor.close()
            return render_template('app.html', sum=message)
