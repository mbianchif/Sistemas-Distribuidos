#!/bin/bash

sudo apt install unzip

DIR=.data
ZIP=dataset.zip

# Delete old .csv files if they exist
for file in movies_metadata.csv ratings.csv credits.csv; do
    path="$DIR/$file"
    [ -f "$path" ] && rm "$path"
done

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