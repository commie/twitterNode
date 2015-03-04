# twitterNode
Node.js based Twitter Streaming API 1.1 Crawler

This is a node.js-based crawler for collecting information from Twitter Streaming API v1.1. It works with oAuth through the use of node.js oAuth library (run "npm install oauth" to install). The crawler handles large volumes of data well, stiches together multi-chunk messages that Twitter sends, and checks data for sanity before writing it to the file. I have ran it for up to 9 months without any problems, with the crawler handling as much as 50 tweets per second.

The output file format is JSON, with individual tweets separated with ",\n" combination that is easy to parse. The crawler automatically creates a new file for every day of data collection, which allows to fetch the necessary batch of tweets very fast. It's dead simple and that is the point.

To use it, populate the included crawler file (i.e. stateCollegeCrawler.js) with Twitter API login information, then run the main file. It checks its buffer every 30 seconds to see if there's at least 1000 tweets collected, then dumps them into a file (to prevent frequent disk access).