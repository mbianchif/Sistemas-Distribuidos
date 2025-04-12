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
unzip -j $DIR/$ZIP "ratings.csv" -d $DIR
unzip -j $DIR/$ZIP "credits.csv" -d $DIR

# Make small versions of the dataset if n was provided
if [ $# -eq 1 ]; then
    N=$(($1 + 1))
    head -n $N "$DIR/movies_metadata.csv" > temp && mv temp "$DIR/movies_metadata.csv"
    head -n $N "$DIR/ratings.csv" > temp && mv temp "$DIR/ratings.csv"
    head -n $N "$DIR/credits.csv" > temp && mv temp "$DIR/credits.csv"
fi