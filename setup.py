import setuptools

setuptools.setup(
    name='passim',
    version='2.0.0-alpha.2',
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
        'graphframes',
        'intervaltree'
    ],
    scripts=['bin/seriatim', 'bin/passim', 'bin/passim.cmd', 'bin/seriatim.cmd'],
    data_files=[('share', ['share/submit-seriatim.py']),
                ('conf', ['conf/log4j2.properties'])],
    python_requires='>=3.6',
)
