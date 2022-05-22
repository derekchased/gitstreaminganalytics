#!/bin/bash

mkdir -p resultsapi/
cd resultsapi/
mkdir -p tmpresults/

#if file not exits, create with initial value
#page variable will be used to save with page are we on
if [ ! -f "page.txt" ]; then
    echo "PAGE=1" > page.txt
fi
source "page.txt"

#iteration variable used to save which iteration of the json are we generating
if [ ! -f "iteration.txt" ]; then
    echo "ITERATION=1" > iteration.txt
fi
source iteration.txt

#TODO change file, to encapsule everything in a while loop that sleeps the corresponding time to reset the API limit 

#the limit is 1000 objects, each call can retrieve a page of maxium 100
# 1000/100 = 10 maxium loop iterations
for i in {1..10}
do
    #the github api has the limit of 100 objects per page 
    #preprocess with jq to get the information we need
    curl -s https://api.github.com/search/repositories\?q\=created:2021-05-01..2022-05-01\&per_page=100\&page=${PAGE} | jq .items > "tmpresults/${i}.json";
    ((PAGE=PAGE+1))
    echo "PAGE=$PAGE" > page.txt
    sleep 0.01
done
jq -s 'add' tmpresults/* > ${ITERATION}.json
((ITERATION=ITERATION+1))
echo "ITERATION=$ITERATION" > iteration.txt
rm tmpresults/*