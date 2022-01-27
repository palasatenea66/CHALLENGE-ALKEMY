CHALLENGE ALKEMY


Author: Anahí Romo (romoanahi@gmail.com)
Python 3.8.10 (used version)
Ubuntu 20.04 'focal fossa' (used environment)

Installation

To run this Challenge, you need some stuff:
1. Python3
2. pip (to install some libraries needed)
3. virtualenv (if you want it)
4. text editor (to modify .ini file as needed)
5. PostgreSQL installed (with username 'postgres' and password 'pepe', you can modify this options in 'config.ini')
6. internet connection :)

a. You can run this code into a virtual environment (venv):

$python3 -m venv /path/to/new/virtual/environment (on Linux)
>c:\Python3\python -m venv c:\path\to\myenv  (on Windows)

b. You must download some libraries, too:

$pip install requests (and: pandas, sqlalchemy, python-decouple, logging, python3-psycopg2) 
There is a 'config.ini' file, here you can change postgres' options and others.

c. You could create tables in the database 'postgres' (or another one you prefer) for this project; for this, you could run 'crear_tablas_postgres.py'. This script creates three tables: 'tabla1', 'tabla2' and 'tabla3_cines'. 

d. This tables will be populated by processed data from dataframes after you run 'challenge_alkemy.py'. The frecuence for download data must be selected by user.

'config.ini' File

This file exists for an easy change of parameters (selected by user or for move website).
Here you can find parameters for font files URL's (about websites to download this) and PostgreSQL options (to configure connection to database engine).

Postgres Options

In 'config.ini', you can set Postgres option for 'host', 'database', 'user' and 'password' to adapt it for your convenience.
For default, host = localhost, database = postgres, user = postgres and password = pepe.



