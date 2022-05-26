#!/bin/bash

mkdir -p resultsapi/
cd resultsapi/
mkdir -p tmpresults/

#if file not exits, create with initial valuex
#date variable will be used to save with date is the last we retrieve information of
if [ ! -f "date.txt" ]; then
    echo "DATE=2021-05-01" > date.txt
fi
source "date.txt" # $DATE later

#retrieve token to access github api (use github token)
source "githubtoken.txt" # $GITHUBTOKEN later

# The Search API has a custom rate limit. For requests using Basic Authentication, 
# you can make up to 30 requests per minute.
# https://docs.github.com/en/rest/search#rate-limit
#
# the limit for each call with pagination is 1000 objects, each page can retrieve a maxium of 100
# https://docs.github.com/en/rest/overview/resources-in-the-rest-api
#
# 1000/100 = 10 maxium calls for each date 
# 30 requests minute / 10 requests each day = 3 days until wait for reset
while true
do
    for i in {1..3}
    do
        #if date variable is up to date, sleep until next day
        if [ "$DATE" = `date '+%C%y-%m-%d'` ]; then
            # calculate seconds until next day at 00:05
            # https://gist.github.com/mowings/a32fbf4dd46d9fb9661822056ceda91c
            SECS=$(expr `date -d "tomorrow 00:05" +%s` - `date -d "now" +%s`)
            sleep $SECS
        fi
        for j in {1..10} # 10*100perpage = 1000 objects per call
        do
            #with more tokens we can do up to 3 days each loop iteration, instead of 1 
            #preprocess a bit with jq to retrieve the information we need
            curl -u FrankJonasmoelle:$GITHUBTOKEN -s https://api.github.com/search/repositories\?q\=created:${DATE}..${DATE}\&per_page=100\&page=${j} | jq .items > "tmpresults/${j}.json";
            sleep 0.01
        done

        jq -s 'add' tmpresults/* > ${DATE}.json
        #https://unix.stackexchange.com/questions/49053/how-do-i-add-x-days-to-date-and-get-new-date
        DATE=`date '+%C%y-%m-%d' -d "$DATE+1days"`
        echo "DATE=$DATE" > date.txt
        rm tmpresults/*
    sleep 0.01
    done
    #sleep some time to reset limit
    sleep 1.50m
done