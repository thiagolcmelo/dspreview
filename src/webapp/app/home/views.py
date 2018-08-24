# python standard
from datetime import datetime

# third-party imports
from flask import jsonify, render_template, request
from flask_login import login_required

# local imports
from . import home
from ..models import Report


@home.route('/')
def homepage():
    """
    Render the homepage template on the / route
    """
    return render_template('home/index.html', title="Welcome")


@home.route('/dashboard')
@login_required
def dashboard():
    """
    Render the dashboard template on the /dashboard route
    """
    return render_template('home/dashboard.html', title="Dashboard")


@home.route('/report')
@login_required
def report_date():
    """
    Return full report data as Json
    """
    try:
        def parse_date(dt):
            if dt:
                try:
                    dt = datetime.strptime(dt, "%Y-%m-%d")
                except Exception:
                    dt = None
            return dt

        start_date = parse_date(request.args.get('start_date',
                                                 default=None, type=None))
        end_date = parse_date(request.args.get('end_date',
                                               default=None, type=None))

        results = []
        if start_date and end_date:
            results = Report.query\
                            .filter(Report.date >= start_date)\
                            .filter(Report.date <= end_date)
        elif start_date:
            results = Report.query.filter(Report.date >= start_date)
        elif end_date:
            results = Report.query.filter(Report.date <= end_date)
        else:
            results = Report.query.all()

        return jsonify({
            "status": "success",
            "data": [r.serialize for r in results]
            })
    except Exception as err:
        print(str(err))
        return jsonify(result={"status": "fail"})
