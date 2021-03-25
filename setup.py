import setuptools

setuptools.setup(
    name='passim',
    version='0.3.0',
    author='David A. Smith',
    author_email='dasmiq@gmail.com',
    description='Detecting and analyzing text reuse',
    url='https://github.com/dasmiq/passim',
    packages=setuptools.find_packages(include=['passim']),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
    ],
    install_requires=[
        'pyspark>=3.0.1',
        'intervaltree'
    ],
    scripts=['bin/seriatim'],
    data_files=[('share', ['share/submit-seriatim.py'])],
    python_requires='>=3.6',
)
