from setuptools import setup, find_packages

with open('README.rst', 'r') as f:
    readme = f.read()

setup(
    name='dspreview',
    description='a simple preview for dsp digital advertising information',
    long_description=readme,
    author='Thiago Melo',
    author_email='thiago.lc.melo@gmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[],
    entry_points={
        'console_scripts': [
            'dspreview=workers.cli:main',
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    url='https://github.com/thiagolcmelo/dspreview',
    download_url='https://github.com/thiagolcmelo/dspreview/archive/0.1.0.tar.gz',
    version='0.1.0',
)
