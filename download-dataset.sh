#!/bin/bash

# Install dependencies
sudo apt install curl

DIR=.data
ZIP=dataset.zip

# Download and unzip dataset
mkdir $DIR
curl -L -o $DIR/$ZIP https://www.kaggle.com/api/v1/datasets/download/rounakbanik/the-movies-dataset
