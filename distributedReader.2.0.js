// 
// TODO: 
// 
// 1. Handle 0d 0a as an individual issue.
// 

var https = require('https');
var http = require('http');
var fs = require('fs');
var sys = require('sys');
var OAuth = require('oauth').OAuth;



var counter = 1;
var tweets = [];

var attemptSalvage = false;
var originalMessage = null;
var originalExpected = null;
var originalObserved = null;

var responseBits = null;

var readerOptions = require('./stateCollegeCrawler.js').options;

var options = {
    host:               'stream.twitter.com',
    path:               '/1.1/statuses/filter.json?locations=' + readerOptions.bbox + '&delimited=length'
};


var defaultTimeout = 2500,
    maxTimeout = 300000,
    timeout = defaultTimeout;

var restarting = false;


function startCrawler () {

    var d = require('domain').create();

    d.on('error', function (e) {

        console.log(generateTimestamp() + '[CRITICAL] [Domain] Error:');
        console.log(e);
        console.log();

        console.error(e);

        d.dispose();
    });

    d.on('dispose', function () {

        if (!restarting) {

            console.log(generateTimestamp() + '[Domain] Old domain disposed of, attempting to restart crawler in a new domain.');
            console.log();

            startCrawler();
        }
    });

    d.run(init);
}


function init () {

    //
    // Launch a new request
    //

    var oa = new OAuth("https://api.twitter.com/oauth/request_token",
        "https://api.twitter.com/oauth/access_token", 
        readerOptions.consumerKey, 
        readerOptions.consumerSecret, 
        "1.0A", 
        "http://www.geovista.psu.edu/SensePlace2/", 
        "HMAC-SHA1"
    );

    var request = oa.get("https://" + options.host + options.path,
        readerOptions.accessToken, 
        readerOptions.accessSecret
        // function (e, data, response) {
        //     if (e) {

        //         console.log(generateTimestamp() + '[CRITICAL] [oa.get] error:');
        //         console.log(e);
        //         console.log();

        //         console.error(e);
        //     }
        // }
    );

    request.end();


    //
    // Register event listeners
    //
    
    request.on('socket', function(socket) {

        // Emitted after a socket is assigned to this request. 
        // 'socket' is an instance of net.Socket

        socket.setKeepAlive(true);
        console.log(generateTimestamp() + "Socket set to keep-alive.");
        console.log();

        socket.on('end', function () {
            console.log(generateTimestamp() + '[CRITICAL] [Socket] "end" event detected - the other end of the socket sent a FIN packet.');
            console.log();
        });

        socket.on('close', function (hadError) {
            console.log(generateTimestamp() + '[CRITICAL] [Socket] "close" event detected. Socket was closed' + (hadError ? '' : ' NOT') + ' due to a transmission error.');
            console.log();
        });

        socket.on('error', function (e) {
            
            console.log(generateTimestamp() + '[CRITICAL] [Socket] "error" event detected:')
            console.log(e);
            console.log();

            console.error(e);
        });

        socket.on('timeout', function () {

            console.log(generateTimestamp() + '[CRITICAL] [Socket] "timeout" event detected.')
            console.log();

            // This is only to notify that the socket has been idle. The user must manually close the connection. 
        });

    });


    request.on('response', function (response) {

        // Emitted when a response is received to this request.
        // 'response' is an instance of http.IncomingMessage

        // Report on new request
        console.log(generateTimestamp() + "Reponse status code: " + response.statusCode + " (" + http.STATUS_CODES[response.statusCode] + ").");
        console.log("Response headers:");
        console.log(response.headers);
        console.log();
        
        
        // Register event listeners

        response.on('data', processResponseData);

        response.on('end', function () {

            console.log(generateTimestamp() + '[CRITICAL] [Response] "end" event detected - stream has received an EOF, and no more "data" events will happen.');
            console.log();

            restart();
        });

        response.on('close', function () {
            console.log(generateTimestamp() + '[CRITICAL] [Response] "close" event detected - the underlying connection was terminated before "end" event was emitted.');
            console.log();
        });

        response.on('error', function (e) {
            
            console.log(generateTimestamp() + '[CRITICAL] [Response] "error" event detected:');
            console.log(e);
            console.log();
        
            console.error(e);
        })

    });


    request.on('error', function(e) {
        
        // Report error
        
        console.log(generateTimestamp() + '[CRITICAL] [Request] "error" event detected:');
        console.log(e);
        console.log();
        
        console.error(e);

        restart();
    });
    
}

function restart () {

    if (!restarting) {

        restarting = true;

        // Set a time-out, then attempt another request
            
        timeout += timeout;

        if (timeout > maxTimeout) { timeout = maxTimeout };

        console.log(generateTimestamp() + "Timeout set, attempting another request in " + (timeout / 1000).toFixed(0) + " seconds.");
        console.log();        

        setTimeout(function() {

            restarting = false;
            
            console.log(generateTimestamp() + "Attempting another request now.");
            console.log();

            init();
            
        }, timeout);
    }
}

function newRequest(options) {

    var oa = new OAuth("https://api.twitter.com/oauth/request_token",
        "https://api.twitter.com/oauth/access_token", 
        readerOptions.consumerKey, 
        readerOptions.consumerSecret, 
        "1.0A", 
        "http://www.geovista.psu.edu/SensePlace2/", 
        "HMAC-SHA1"
    );

    var request = oa.get("https://" + options.host + options.path,
        readerOptions.accessToken, 
        readerOptions.accessSecret
    );
    
    return request;
}

function processResponseData(d) {

    if (d.length > 4) {

        // If an attempt at salvage is in progress,
        if (attemptSalvage) {

            // .. check if the most recent broken tweet can be mended with 
            // the previous one to produce the expected message size.


            // Measure the total length of the response bits accumulated so far

            var numberOfBits = responseBits.length,
                currentResponseLength = 0,
                i;

            for (i = 0; i < numberOfBits; i++) {
                currentResponseLength += responseBits[i].length;
            }


            // Attempt to mend the message

            if (originalExpected > currentResponseLength + d.length) {

                // We might be a few chunks short - continue with the salvage attempt

                responseBits.push(d);
                //console.log("\tAcquired another chunk, continuing the salvage attempt.");

            }

            if (originalExpected == currentResponseLength + d.length) {

                // If so, mend the tweets and store the result.

                // console.log("\tMessage can be mended, collecting tweet #" + counter + "..");

                responseBits.push(d);
                numberOfBits = responseBits.length;

                var mendedMessage = "";

                for (i = 0; i < numberOfBits; i++) {
                    mendedMessage += responseBits[i];
                }

                try {

                    JSON.parse(mendedMessage);

                    // If message parsed alright, store it

                    tweets.push(mendedMessage);

                    
                    
                    // console.log();
                    // console.log("\t.. done.");

                    console.log("Collected tweet #" + counter + " in " + numberOfBits + " chunks.");
                    console.log();

                    counter++;

                } catch (e) {

                    console.log("\tError - mended message failed to parse:");
                    console.log(mendedMessage);
                }

                attemptSalvage = false;
            }

            if (originalExpected < currentResponseLength + d.length) {

                // We've got extra pieces in the salvage buffer - abort the salvage attempt
                console.log("\tTotal length of chunks exceeded expected message length, aborting the salvage attempt.");
                
                attemptSalvage = false;
                processResponseData(d);
            }

        } else {

            // Fetch the expected and the observed size of the 
            var expectedLength = parseInt(d.slice(0, 4).toString());                // First 4 bytes have the length, next 2 have CR+LF.
            var observedLength = d.length - 6;

            // If message size does not check out ..
            if (expectedLength != observedLength) {

                // Initiate salvage attempt

                // console.log("Message size problem - " + expectedLength + " bytes expected, " + observedLength + " observed. Initiating the salvage attempt.");

                responseBits = [d.slice(6, d.length)];

                originalExpected = expectedLength;
                attemptSalvage = true;

            } else {

                // If message size checks out, store it.

                tweets.push(d.slice(6, d.length));

                console.log("Collected tweet #" + counter + ".");
                console.log();
                //console.log(d.slice(6, d.length).toString());

                counter++;

                // // Reset the timeout
                // if (timeout != defaultTimeout) {

                //     console.log(generateTimestamp() + "Timeout reset to " + (timeout / 1000).toFixed(0) + " seconds.");
                //     timeout = defaultTimeout;
                // }

            }
            
        }
        
        // console.log();

    } else {

        console.log(generateTimestamp() + '[CRITICAL] [Response] Something is wrong with response data:');
        console.log(d);
        console.log();

        attemptSalvage = false;
    }

}

function generateTimestamp() {
    
    // Generate timestamp in the form of "Tue Sep 11 2012 12:54:00 "
    
    var timeStamp = new Date();
    var timeString = timeStamp.toDateString() + " " + timeStamp.toLocaleTimeString() + " ";
    
    return timeString;
}

//
// Start the tweet collector
//
    
setInterval(function() {

    var tweetCount = tweets.length;
    
    var timeStamp = new Date();
    var timeString = timeStamp.toDateString() + " " + timeStamp.toLocaleTimeString();

    if (tweetCount > 1000) {

        var tweetList = tweets.splice(0, tweetCount);

        var tweetString = tweetList.reduce(function(previousValue, currentValue) {
            return previousValue.toString() + ",\n" + currentValue.toString();
        });

        tweetString += ",\n";

        var year        = timeStamp.getUTCFullYear(),
            month       = (timeStamp.getUTCMonth() < 9) ? "0" + (timeStamp.getUTCMonth() + 1) : (timeStamp.getUTCMonth() + 1),
            day         = (timeStamp.getUTCDate() < 10) ? "0" + timeStamp.getUTCDate() : timeStamp.getUTCDate(),
            fileName    = "distributedReader.2.0." + readerOptions.name + "." + year + "." + month + "." + day + ".out";

        fs.appendFileSync(fileName, tweetString);

        console.log(generateTimestamp() + "- " + tweetCount + " tweets dumped.");
        console.log();

        // Reset the timeout
        if (timeout != defaultTimeout) {

            timeout = defaultTimeout;

            console.log(generateTimestamp() + "Timeout reset to " + (timeout / 1000).toFixed(0) + " seconds.");
            console.log();
        }

        // ///////////////////////////////////////////////////// //

        try {

        } catch (e) {

            console.log("\tError - failed to write data to disk:");
            console.log(e);

            console.error("\tError - failed to write data to disk:");
            console.error(e);
        }



    } else {

        console.log(generateTimestamp() + "- " + "less than 1000 tweets in the buffer.");
        console.log();
    }

}, 30000);

startCrawler();