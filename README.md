# IMDb Dataset Setup Guide

This project requires a subset of the IMDb dataset to build the local
SQLite database used by the server.

Follow the steps below to install dependencies, download the dataset
using the Kaggle CLI, place the required files into the correct
directory, and generate the database.

------------------------------------------------------------------------

# Requirements

You must have:

-   Python 3 installed
-   pip installed
-   Internet connection
-   Kaggle account

------------------------------------------------------------------------

# Step 1 --- Install Kaggle CLI

Install the Kaggle command-line interface:

``` bash
pip install kaggle
```

Verify installation:

``` bash
kaggle --version
```

------------------------------------------------------------------------

# Step 2 --- Configure Kaggle API Key

1.  Log in to Kaggle
2.  Go to:

https://www.kaggle.com/settings/account

3.  Click:

Create New API Token

4.  Download:

kaggle.json

Move the API key into the correct directory:

``` bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

Test access:

``` bash
kaggle datasets list
```

If this command prints datasets, authentication is working.

------------------------------------------------------------------------

# Step 3 --- Download the Dataset

Run the following command from the project root directory:

``` bash
kaggle datasets download ashirwadsangwan/imdb-dataset \
  -f movies.csv \
  --unzip
```

------------------------------------------------------------------------

# Step 4 --- Required Files

This project only requires the following IMDb files:

    title.basics.tsv
    title.ratings.tsv

------------------------------------------------------------------------

# Step 5 --- Move Files into server/

Move the required files into the server directory:

``` bash
mv title.basics.tsv server/
mv title.ratings.tsv server/
```

Your directory structure should look like:

    project-root/

    server/
        imdb_to_db.py
        title.basics.tsv
        title.ratings.tsv

    README.md

------------------------------------------------------------------------

# Step 6 --- Generate the Database

Run the database conversion script:

``` bash
python server/imdb_to_db.py
```

This script will create:

    server/imdb.db

------------------------------------------------------------------------

# Step 7 --- Run the script

``` bash
python main.py
```
This script will take host and ip number as argument
(default host is 127.0.0.1 and default post is 8000)

------------------------------------------------------------------------


# Quick Setup (One Command)

``` bash
pip install kaggle && \
kaggle datasets download ashirwadsangwan/imdb-dataset \
  -f movies.csv \
  --unzip && \
mv title.basics.tsv server/ && \
mv title.ratings.tsv server/ && \
python server/imdb_to_db.py
sqlite3 server/imdb.db server/make_movies.sql
python main.py
```

------------------------------------------------------------------------

# Notes

Do not commit large dataset files or databases to Git.

Add the following to `.gitignore`:

    imdb.db
    *.tsv
    *.csv
