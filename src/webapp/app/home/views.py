# python standard
from datetime import datetime

# third-party imports
from flask import flash, redirect, render_template, url_for, jsonify, request
from flask_login import login_required

# local imports
from . import home
from .forms import ClassificationForm
from .. import db
from ..models import Report, Classification, DCM, DSP
from ..queries import GENERATE_CLASSIFIED, GENERATE_REPORT


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
        return jsonify({
            "status": "fail",
            "data": []
        })


@home.route('/classifications', methods=['GET', 'POST'])
@login_required
def list_classifications():
    """
    List all classifications
    """
    classifications = Classification.query.all()

    return render_template('home/classifications.html',
                           classifications=classifications,
                           title="Classifications")


@home.route('/classifications/add', methods=['GET', 'POST'])
@login_required
def add_classification():
    """
    Add a classification to the database
    """
    add_classification = True

    form = ClassificationForm()
    if form.validate_on_submit():
        classification = Classification(pattern=form.pattern.data,
                                        brand=form.brand.data,
                                        sub_brand=form.sub_brand.data,
                                        dsp=form.dsp.data,
                                        use_campaign_id=form.use_campaign_id.data,
                                        use_campaign=form.use_campaign.data,
                                        use_placement_id=form.use_placement_id.data,
                                        use_placement=form.use_placement.data)
        try:
            # add classification to the database
            db.session.add(classification)
            db.session.commit()
            flash('You have successfully added a new classification.')
        except Exception:
            # in case classification name already exists
            flash('Error: classification name already exists.')

        # redirect to classifications page
        return redirect(url_for('home.list_classifications'))

    # load classification template
    return render_template('home/classification.html', action="Add",
                           add_classification=add_classification, form=form,
                           title="Add Classification")


@home.route('/classifications/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_classification(id):
    """
    Edit a classification
    """

    add_classification = False

    classification = Classification.query.get_or_404(id)
    form = ClassificationForm(obj=classification)
    if form.validate_on_submit():
        classification.brand = form.brand.data
        classification.sub_brand = form.sub_brand.data
        classification.dsp = form.dsp.data
        classification.use_campaign_id = form.use_campaign_id.data
        classification.use_campaign = form.use_campaign.data
        classification.use_placement_id = form.use_placement_id.data
        classification.use_placement = form.use_placement.data
        db.session.commit()
        flash('You have successfully edited the classification.')

        # redirect to the classifications page
        return redirect(url_for('home.list_classifications'))

    form.brand.data = classification.brand
    form.sub_brand.data = classification.sub_brand
    form.dsp.data = classification.dsp
    form.use_campaign_id.data = classification.use_campaign_id
    form.use_campaign.data = classification.use_campaign
    form.use_placement_id.data = classification.use_placement_id
    form.use_placement.data = classification.use_placement

    return render_template('home/classification.html', action="Edit",
                           add_classification=add_classification, form=form,
                           classification=classification,
                           title="Edit Classification")


@home.route('/classification/delete/<int:id>', methods=['GET', 'POST'])
@login_required
def delete_classification(id):
    """
    Delete a classification from the database
    """
    classification = Classification.query.get_or_404(id)
    db.session.delete(classification)
    db.session.commit()
    flash('You have successfully deleted the classification.')

    # redirect to the classifications page
    return redirect(url_for('home.list_classifications'))

    return render_template(title="Delete Classification")


@home.route('/classification/reset', methods=['GET'])
@login_required
def reset_classifications():
    """
    Reset all classifications, might take a while
    """
    try:
        DCM.query.delete()
        DSP.query.delete()
        Report.query.delete()
        db.session.execute(GENERATE_CLASSIFIED)
        db.session.execute(GENERATE_REPORT)
        db.session.commit()
        return jsonify({
            "status": "success"
        })
    except Exception as err:
        print(str(err))
        return jsonify({
            "status": "fail"
        })
