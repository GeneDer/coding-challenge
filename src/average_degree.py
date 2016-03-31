# Written by Gene Der Su for Insight Coding Challenge.

#import libraries
import sys
import json
import time
import bisect


def extractData(parsedTweet):
    """
        This function extrct hashtags and created_at from a parsed tweet.
        It then converts the created_at into timestamp. It returns a
        list of hashtags and an integer timestamp.

        input:
            parsedTweet - a tweet parsed using json.
            
        output:
            hashtags - a list of hashtags from the tweet.

            timestamp - an integer of created_at converted to seconds
            since epoch.
    """

    #extract hashtags as a list
    hashtags = [x['text'] for x in parsedTweet['entities']['hashtags']]

    #extract created_at and convert into an integer of seconds since epoch
    timestamp = int(time.mktime(time.strptime(parsedTweet['created_at'][0:20] +\
                                              parsedTweet['created_at'][26:],
                                              '%a %b %d %H:%M:%S %Y')))
    return hashtags, timestamp


def updataGraph(hashtags, timestamp, maxTimestamp,
                timestamps, tweetsWithin60s, edges):
    """
        This functoin will updata the graph with the data
        extracted from a new tweet.

        input:
            hashtags - list of hashtags in the new tweet.
            
            timestamp - integer of seconds since epoch for the new tweet.
            
            maxTimestamp - integer of the time of current lastest tweet.
            
            timestamps - a sorted list of unique timestamps within 60s of
                         latest tweet.
                         
            tweetsWithin60s - a dictionary of timestamps as keys and
                              list of hashtags as values for tweets
                              within 60s of latest tweet.
            edges - a dictionary of each pair of graph as well as the number
                    of tweets formed such edges. The of tweet is used to
                    determine if an edge should be removed.
                    
        output:
            maxTimestamp - updated time for the latest tweet.

            timestamps - updated list of sorted timestamps within 60s
                         of the latest tweet.

            tweetsWithin60s - updated dictionary of timestamps as keys
                              and list of hashtags as values for tweets
                              within 60 of the latest tweet.

            edges - updated dictionary of edges formed from tweets within 60s
                    of the lastest tweet.
    """

    #new tweet arrived in order of time
    if (timestamp >= maxTimestamp):

        #only append timestamp if not already in the list
        if (timestamp != maxTimestamp):
            timestamps.append(timestamp)
            maxTimestamp = timestamp

        #deal with the possiblility of same timestamp
        if timestamp in tweetsWithin60s:
            tweetsWithin60s[timestamp].append(hashtags)
        else:
            tweetsWithin60s[timestamp] = [hashtags]
            
        #only add edges if there are 2 or more hashtages
        if (len(hashtags) > 1):
            for i in hashtags:
                for j in hashtags:
                    if (i != j):
                        if i in edges:
                            if j in edges[i]:
                                edges[i][j] += 1
                            else:
                                edges[i][j] = 1
                        else:
                            edges[i] = {j:1}

        #removing edges
        while (timestamps[0] + 60 < maxTimestamp):
            for ht in tweetsWithin60s[timestamps[0]]:
                for i in ht:
                    for j in ht:
                        if (i != j):
                            edges[i][j] -= 1
                            if (edges[i][j] == 0):
                                edges[i].pop(j)
                                if(edges[i]=={}):
                                    edges.pop(i)
            tweetsWithin60s.pop(timestamps[0])
            timestamps.pop(0)
            
    #new tweet arrived out of order but within 60s.                               
    elif (timestamp + 60 >= maxTimestamp):

        #only append timestamp if not already in the list.
        #also keep the listed sorted
        index = bisect.bisect_left(timestamps,timestamp)
        if (timestamp != timestamps[index]):
            timestamps.insert(index, timestamp)
            
        #deal with the possiblility of same timestamp
        if timestamp in tweetsWithin60s:
            tweetsWithin60s[timestamp].append(hashtags)
        else:
            tweetsWithin60s[timestamp] = [hashtags]
            
        #only add edges if there are 2 or more hashtages
        if (len(hashtags) > 1):
            for i in hashtags:
                for j in hashtags:
                    if (i != j):
                        if i in edges:
                            if j in edges[i]:
                                edges[i][j] += 1
                            else:
                                edges[i][j] = 1
                        else:
                            edges[i] = {j:1}
                            
    #do nothing if new tweet arrived out of order and without 60s
    else:
        pass
    
    return maxTimestamp, timestamps, tweetsWithin60s, edges
            

def main(inputFile, outputFile):
    """
        This program will run through each tweet in the
        inputFile and calculate the average degree of
        hashtag. It will writes each calculated average
        degree to outputFile.
    """

    #initialize objects
    maxTimestamp = -1
    timestamps = []
    tweetsWithin60s = {}
    edges = {}

    with open(inputFile,'r') as tweets, open(outputFile,'w') as degrees:
        
        #loop through each line of inputFile
        for tweet in tweets:

            #parse tweet with json
            parsedTweet = json.loads(tweet)
            
            #make sure the current line is not a rate limiting message
            if 'limit' not in parsedTweet:

                #extract hashtages and timestamp 
                hashtags, timestamp = extractData(parsedTweet)

                #updata graph with extracted data
                (maxTimestamp, timestamps,
                 tweetsWithin60s, edges) = updataGraph(hashtags,timestamp,
                                                       maxTimestamp, timestamps,
                                                       tweetsWithin60s, edges)

                #try to calculate the degree, if graph is empty then degree = 0
                try:
                    degree = 100*sum(len(edges[x]) for x in edges)/len(edges)
                except ZeroDivisionError:
                    degree = 0

                #write the degree to outputFile
                degrees.write('%.2f\n'%(degree/100.0))
    

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])



