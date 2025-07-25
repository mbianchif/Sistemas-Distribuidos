#!/bin/bash

# Install dependencies
sudo apt install curl
sudo apt install unzip

DIR=.data
ZIP=dataset.zip

# Download and unzip dataset
mkdir $DIR
curl -L -o $DIR/$ZIP https://www.kaggle.com/api/v1/datasets/download/rounakbanik/the-movies-dataset

# Extract files
unzip -j $DIR/$ZIP "movies_metadata.csv" -d $DIR
unzip -j $DIR/$ZIP "credits.csv" -d $DIR
unzip -j $DIR/$ZIP "ratings.csv" -d $DIR

# Rename movies
mv $DIR/movies_metadata.csv $DIR/movies.csv
