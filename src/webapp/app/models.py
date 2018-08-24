# third-party imports
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy.sql import func
from sqlalchemy import Index

# local imports
from webapp.app import db, login_manager


class DCM(db.Model):
    """
    Create a DCM classified table
    """

    __tablename__ = 'dcm_classified'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    campaign_id = db.Column(db.Integer, nullable=False)
    campaign = db.Column(db.String(75), nullable=False)
    placement_id = db.Column(db.Integer, nullable=False)
    placement = db.Column(db.String(75), nullable=False)
    impressions = db.Column(db.Float, nullable=False)
    clicks = db.Column(db.Integer, nullable=False)
    reach = db.Column(db.Float, nullable=False)
    brand = db.Column(db.String(25), nullable=False)
    sub_brand = db.Column(db.String(25), nullable=False)
    dsp = db.Column(db.String(25), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now(), onupdate=func.now())


# Create an index to not allow reapeated values on these dimensions
Index('dcm_classified_index', DCM.date, DCM.brand, DCM.sub_brand,
      DCM.campaign_id, DCM.campaign, DCM.placement_id, DCM.placement, DCM.dsp,
      unique=True)


class DCMRaw(db.Model):
    """
    Create a DCM raw table
    """

    __tablename__ = 'dcm_raw'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    campaign_id = db.Column(db.Integer, nullable=False)
    campaign = db.Column(db.String(75), nullable=False)
    placement_id = db.Column(db.Integer, nullable=False)
    placement = db.Column(db.String(75), nullable=False)
    impressions = db.Column(db.Float, nullable=False)
    clicks = db.Column(db.Integer, nullable=False)
    reach = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now(), onupdate=func.now())


# Create an index to not allow reapeated values on these dimensions
Index('dcm_raw_index', DCMRaw.date, DCMRaw.campaign_id, DCMRaw.campaign,
      DCMRaw.placement_id, DCMRaw.placement, unique=True)


class DSP(db.Model):
    """
    Create a DSP classified table
    """

    __tablename__ = 'dsp_classified'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    campaign_id = db.Column(db.Integer, nullable=False)
    campaign = db.Column(db.String(75), nullable=False)
    impressions = db.Column(db.Float, nullable=False)
    clicks = db.Column(db.Integer, nullable=False)
    cost = db.Column(db.Float, nullable=False)
    brand = db.Column(db.String(25), nullable=False)
    sub_brand = db.Column(db.String(25), nullable=False)
    dsp = db.Column(db.String(25), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now(), onupdate=func.now())


# Create an index to not allow reapeated values on these dimensions
Index('dsp_classified_index', DSP.date, DSP.brand, DSP.sub_brand,
      DSP.campaign_id, DSP.campaign, DSP.dsp, unique=True)


class DSPRaw(db.Model):
    """
    Create a DSP raw table
    """

    __tablename__ = 'dsp_raw'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    campaign_id = db.Column(db.Integer, nullable=False)
    campaign = db.Column(db.String(75), nullable=False)
    impressions = db.Column(db.Float, nullable=False)
    clicks = db.Column(db.Integer, nullable=False)
    cost = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now(), onupdate=func.now())


# Create an index to not allow reapeated values on these dimensions
Index('dsp_raw_index', DSPRaw.date, DSPRaw.campaign_id, DSPRaw.campaign,
      unique=True)


class Report(db.Model):
    """
    Create a Report table
    """

    __tablename__ = 'report'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    brand = db.Column(db.String(25), nullable=False)
    sub_brand = db.Column(db.String(25), nullable=False)
    ad_campaign_id = db.Column(db.Integer, nullable=False)
    ad_campaign = db.Column(db.String(75), nullable=False)
    dsp = db.Column(db.String(25), nullable=False)
    dsp_campaign_id = db.Column(db.Integer, nullable=False)
    dsp_campaign = db.Column(db.String(75), nullable=False)
    ad_impressions = db.Column(db.Float, nullable=False)
    ad_clicks = db.Column(db.Integer, nullable=False)
    ad_reach = db.Column(db.Float, nullable=False)
    dsp_impressions = db.Column(db.Float, nullable=False)
    dsp_clicks = db.Column(db.Integer, nullable=False)
    dsp_cost = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now(), onupdate=func.now())

    @property
    def serialize(self):
        """Return object data in serializeable format"""
        return {
            "date": self.date.isoformat(),
            "brand": self.brand,
            "sub_brand": self.sub_brand,
            "ad_campaign_id": self.ad_campaign_id,
            "ad_campaign": self.ad_campaign,
            "dsp": self.dsp,
            "dsp_campaign_id": self.dsp_campaign_id,
            "dsp_campaign": self.dsp_campaign,
            "ad_impressions": self.ad_impressions,
            "ad_clicks": self.ad_clicks,
            "ad_reach": self.ad_reach,
            "dsp_impressions": self.dsp_impressions,
            "dsp_clicks": self.dsp_clicks,
            "dsp_cost": self.dsp_cost,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
            }


# Create an index to not allow reapeated values on these dimensions
Index('report_index', Report.date, Report.brand, Report.sub_brand,
      Report.ad_campaign_id, Report.ad_campaign, Report.dsp,
      Report.dsp_campaign_id, Report.dsp_campaign, unique=True)


class Classification(db.Model):
    """
    Create a classification model
    """

    __tablename__ = 'classifications'
    id = db.Column(db.Integer, primary_key=True)
    pattern = db.Column(db.String(256), index=True)
    brand = db.Column(db.String(25), nullable=True)
    sub_brand = db.Column(db.String(25), nullable=True)
    dsp = db.Column(db.String(25), nullable=True)
    use_campaign_id = db.Column(db.Boolean, nullable=False, default=False)
    use_campaign = db.Column(db.Boolean, nullable=False, default=False)
    use_placement_id = db.Column(db.Boolean, nullable=False, default=False)
    use_placement = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False,
                           server_default=func.now(), onupdate=func.now())


# Create an index to not allow reapeated values on these dimensions
Index('classification_index', Classification.pattern, Classification.brand,
      Classification.sub_brand, Classification.dsp,
      Classification.use_campaign, Classification.use_campaign_id,
      Classification.use_placement, Classification.use_placement_id,
      unique=True)


class User(UserMixin, db.Model):
    """
    Create an User table
    """

    # Ensures table will be named in plural and not in singular
    # as is the name of the model
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(60), index=True, unique=True)
    username = db.Column(db.String(60), index=True, unique=True)
    first_name = db.Column(db.String(60), index=True)
    last_name = db.Column(db.String(60), index=True)
    password_hash = db.Column(db.String(128))
    is_admin = db.Column(db.Boolean, default=False)

    @property
    def password(self):
        """
        Prevent pasword from being accessed
        """
        raise AttributeError('password is not a readable attribute.')

    @password.setter
    def password(self, password):
        """
        Set password to a hashed password
        """
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        """
        Check if hashed password matches actual password
        """
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return '<User: {}>'.format(self.username)


# Set up user_loader
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))
