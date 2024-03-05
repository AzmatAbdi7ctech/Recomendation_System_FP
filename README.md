# FashionPass 
## Our Picks for You
[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://bitbucket.org/ubaidnizami/fp-ai/src/master/)
Recommender systems are algorithms that suggest relevant items to users based on data.
## Features
- Daily bases product similarity is calculated. 
- New tags can easily add. 
- New model modification easy to do.
- Product similarity version is getting saved
- Project is runing on docker 

## Tech
Following technologies are using:
- Pyspark -- Big Data Processing.
- Pandas  -- Data Manipulation.
- nltk    -- Natural Language Processing.
- sklearn -- Machine Learning Operation.
- sentence-transformers -- Generate Embeddings
- boto3 -- Perform Operaton in S3
- configparser -- Helps in config.ini file oprations
- numpy -- Perform a wide variety of mathematical operations on arrays
- python-dotenv --  Helps in .env file oprations
## Installation

This Project is required [Python 3.11.5](https://www.anaconda.com/installation-success?source=installer) you need to install anaconda Navigator

Install the dependencies.
### For Local environments
```sh
conda create -n RS
conda activate RS
conda install --file requirements.txt
```
If some packages not installed than install individual package
```sh
conda install conda-forge::findspark
```
### For production environments...
```sh
cd Project Directory
sudo docker build -t recsys . #Build the image.
./rs_scritps.sh #This script run the image and performed all task.
 sudo docker run -it recsys sh  #If you want to run docker in inteactive mode.
```

## Directory Structure
```bash
└───fp-ai
    ├───Code
    │   ├───data        
    │   │   ├───dataprocessing
    │   │   │   ├───extract     #Extract the data from the server
    │   │   │   │   └───__pycache__
    │   │   │   ├───load     #Dump the data into project Databse directory
    │   │   │   │   └───__pycache__
    │   │   │   └───transform #Transformation in csv fill
    │   │   │       └───__pycache__
    │   │   └───feartures  #Fetures directory 
    │   ├───models
    │   │   └───model_training    #Model  training
    │   │       └───__pycache__
    │   ├───notebook    #Raw  implementation
    │   └───utils  #All packages and logs
    │       └───__pycache__
    └───__pycache__
```
