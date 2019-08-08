from setuptools import setup, find_packages

from elb_log_ingestor.__version__ import VERSION


setup(
    name="elb_log_ingestor",
    zip_safe=False,
    version=VERSION,
    description="Ships ALB and ELB logs from S3 to Elasticsearch",
    long_description=open("README.md", "r", encoding="utf-8").read(),
    author="cloud.gov Operations Team",
    author_email="cloud-gov-operations@gsa.gov",
    license="Public Domain",
    url="https://github.com/18F/cg-elb-log-ingestor",
    include_package_data=True,
    packages=find_packages(),
    entry_points={
        "console_scripts": "elb_log_ingestor=elb_log_ingestor.main:start_server"
    },
    install_requires=["boto3", "elasticsearch>=6.0.0,<7.0.0"],
    setup_requires=["pytest_runner"],
    tests_require=open("requirements-dev.txt", "r").read().strip().split("\n"),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Topic :: Utilities",
    ],
)
