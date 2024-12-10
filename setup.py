from setuptools import find_packages, setup

NAME = "sqlalchemy-kusto"
DESCRIPTION = "Azure Data Explorer (Kusto) dialect for SQLAlchemy"
VERSION = "3.0.0"


REQUIREMENTS = [
    "azure-kusto-data==4.*",
    "sqlalchemy==1.4.*",
    "typing-extensions>=3.10",
]
EXTRAS = {
    "dev": [
        "black>=24.8.0",
        "isort>=5.13.2",
        "mypy==1.13.0",
        "pylint==3.3.2",
        "pytest>=8.3.4",
        "python-dotenv>=1.0.1",
    ]
}

with open("README.md", "r", encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

setup(
    author="Dodo Engineering",
    author_email="devcommunity@dodopizza.com",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
    description=DESCRIPTION,
    entry_points={
        "sqlalchemy.dialects": [
            "kustosql.https = sqlalchemy_kusto.dialect_sql:KustoSqlHttpsDialect",
            "kustokql.https = sqlalchemy_kusto.dialect_kql:KustoKqlHttpsDialect",
        ]
    },
    extras_require=EXTRAS,
    include_package_data=True,
    install_requires=REQUIREMENTS,
    license="Apache License, Version 2.0",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    name=NAME,
    url="https://github.com/dodopizza/sqlalchemy-kusto",
    packages=find_packages(exclude=["tests", "tests.*"]),
    project_urls={
        "Bug Tracker": "https://github.com/dodopizza/sqlalchemy-kusto/issues",
    },
    python_requires=">=3.8",
    version=VERSION,
    zip_safe=False,
)
