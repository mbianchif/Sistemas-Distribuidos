#!/bin/bash

# Install dependencies
sudo apt install curl
sudo apt install unzip

# Download and unzip dataset
mkdir .data
curl -L -o .data/dataset.zip https://www.kaggle.com/api/v1/datasets/download/rounakbanik/the-movies-dataset
unzip .data/dataset.zip -d .data
