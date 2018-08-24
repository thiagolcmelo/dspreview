# -*- coding: utf-8 -*-

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, BooleanField
from wtforms.validators import DataRequired


class ClassificationForm(FlaskForm):
    """
    Form for adding, editing, removing classifications
    """
    pattern = StringField('Pattern', validators=[DataRequired()],
                          render_kw={"placeholder": "Regular Expression..."})
    brand = StringField('Brand', render_kw={"placeholder": "Brand..."})
    sub_brand = StringField('Sub Brand',
                            render_kw={"placeholder": "Sub Brand..."})
    dsp = StringField('DSP', render_kw={"placeholder": "DSP..."})
    use_campaign_id = BooleanField('Use Campaign ID')
    use_campaign = BooleanField('Use Campaign')
    use_placement_id = BooleanField('Use Placement ID (DCM)')
    use_placement = BooleanField('Use Placement (DCM)')
    submit = SubmitField('Submit')
