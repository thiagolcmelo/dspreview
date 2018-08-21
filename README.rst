dspreview
=========

Tool for merging DSP data from many providers in a single view. There are CLI
tools for launching the workers responsible for parsing ``.csv`` files and 
storing them in a MySQL database. There is also a web app where it is possible
to have a complete report of the operation.

Usage
-----

You must specify the following environment variables prior to usage:

- ``DB_HOST`` the url or ip for the database
- ``DB_NAME`` the name of the database
- ``DB_USER`` a user with writing permission
- ``DB_PASS`` the user's password

Since this project has support only for GCP (currently), the following 
environment variables are also mandatory:

- ``GCP_BUCKET`` the bucket where the ``.csv`` file will be placed
- ``GCP_BUCKET_ARCHIVE`` the bucket where the ``.csv`` file will be archive after processed.
- ``GCP_SERVICE_ACCOUNT_JSON`` the json file for an account with admin permissions for the `Storage`_ service.

If the above environment variables are set, you can initialize the system.
It will create the database, tables, and so on. It might be donne through:

::

    $ dspreview init


There are currently two workers: ``dcm`` and ``dsp``. The ``dcm`` worker expects
to find a file named ``dcm.csv`` inside the ``GCP_BUCKET``, with the 
following structure:

    **[date, campaign_id, campaign, placement_id, placement, impressions, clicks, reach]**

where:

- ``date`` should be in format YYYY-MM-DD
- ``campaign_id`` is an integer
- ``campaign`` is a string with the information ``brand_subbrand``
- ``placement_id`` is an integer
- ``placement`` is a string with the information ``dsp_adtype``
- ``impressions`` is an integer
- ``clicks`` is an integer
- ``reach`` is an float, please take care to not repeat this, since it is a calculated metric

While the ``dsp`` worker expect to find a file with the dsp's name (like
``dbm.csv`` or ``mediamath.csv``) and the following structure:

    **[date, campaign_id, campaign, impressions, clicks, cost]**

where:

- ``date`` should be in format YYYY-MM-DD
- ``campaign_id`` is an integer
- ``campaign`` is a string with the information ``brand_subbrand_adtype``
- ``impressions`` is an integer
- ``clicks`` is an integer
- ``cost`` is a float

In order to launch a worker, you might use the command:

:: 

    $ dspreview --worker dcm

or:

::

    $ dspreview --worker dsp


If the DSP is known in advance, you might run:

::

    $ dspreview --worker dsp --dsp dbm

or

::

    $ dspreview --worker dsp --dsp mediamath


When all files are stored in the MySQL database, the following command generates
the full report:

::

    $ dspreview --generate-report

The web app might be run through:

::

    $ dspreview serve --port 80

The default port is ``80``


Preparing for Development
-------------------------

1. Ensure ``pip`` and ``pipenv`` are installed.
2. Clone repository: ``https://github.com/thiagolcmelo/dspreview``
3. Fetch development dependencies: ``make install``


Running Tests
-------------

Run tests locally using ``make`` if virtualenv is active:

::

    $ make

If virtualenv isn't active then use

::

    $ pipenv run make

.. _Storage: https://cloud.google.com/storage/
.. _SQL: https://cloud.google.com/sql/