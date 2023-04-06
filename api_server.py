import datetime
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from flask_restful import Api
import cnb_get
import database
import config

app = Flask(__name__)
api = Api(app)

def db_adding(day):
    daily_rates = cnb_get.get_values(day)
    database.insert_rates(day, daily_rates)

def initialization_schedule():
    scheduler = BackgroundScheduler()
    if config.schedule['interval'] == 'everyday':
        scheduler.add_job(lambda: db_adding(datetime.date.today()), 'interval', days=1)
    elif config.schedule['interval'] == 'every hour':
        scheduler.add_job(lambda: db_adding(datetime.date.today()), 'interval', hours=1)
    scheduler.start()

@app.route('/cnb_api/report')
def api_select():
    try:
        day_start = request.args.get('day_start')
        day_finish = request.args.get('day_finish')
        if not day_start or not day_finish:
            return {'error': 'day_start and end_date are required'}, 400
        try:
            day_s = datetime.datetime.strptime(day_start, '%Y-%m-%d').date()
            day_f = datetime.datetime.strptime(day_finish, '%Y-%m-%d').date()
        except ValueError:
            return {'error': 'Invalid date format'}, 400
        delta = datetime.timedelta(days=1)
        day_s_copy = day_s
        while (day_s_copy <= day_f):
            db_adding(day_s_copy)
            day_s_copy += delta
        result_db = database.get_rates(day_s, day_f)
        output = {currency: {'min': min_rate, 'max': max_rate, 'avg': avg_rate} for currency, min_rate, max_rate, avg_rate in result_db}
        return jsonify(output)
    except (Exception):
        return {'error': 'Error'}, 400

@app.route('/cnb_api/day')
def api_day():
    try:
        day = request.args.get('day')
        if not day:
            return {'error': 'day are required'}, 400
        try:
            day_right = datetime.datetime.strptime(day, '%Y-%m-%d').date()
        except ValueError:
            return {'error': 'Invalid date format'}, 400
        db_adding(day_right)
        result_db = database.get_one_rate(day_right)
        output = {currency: {'rate': rate} for currency, rate in result_db}
        return jsonify(output)
    except (Exception):
        return {'error': 'Error'}, 400

@app.route('/cnb_api/db')
def api_db():
    database.create_table()
    return {'DataBase status': 'DataBase is run'}

if __name__ == '__main__':
    database.create_table()
    db_adding(datetime.date.today())
    initialization_schedule()
    app.run(debug=True)