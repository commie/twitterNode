var https   = require('https'),
    http    = require('http'),
    fs      = require('fs'),
    sys     = require('sys'),
    OAuth   = require('oauth').OAuth,
    commandLineParser = require('command-line-args');

// grab command line options

var optionDefinitions = [
    { name: "netwInterface", type: String },
    { name: "crawlerName", type: String}
];

var comLineOptions = commandLineParser(optionDefinitions);


// initialize crawler variables

// var readerOptions = require('./crawler.01.js').options;
var readerOptions = require("./" + comLineOptions.crawlerName + ".js").options;


var options = {
    host:   'stream.twitter.com',
    path:   '/1.1/statuses/filter.json?' + 
            // 'locations=' + readerOptions.bbox + 
            '&' + 'delimited=length' + 
            '&' + 'stall_warnings=true'
};

if (readerOptions.bbox) {
    options.path += '&' + 'locations=' + readerOptions.bbox
}

if (readerOptions.keyw) {
    options.path += '&' + 'track=' + readerOptions.keyw
}

var defaultTimeout  = 2500,
    maxTimeout      = 300000,
    timeout         = defaultTimeout,
    restarting      = false;

var tweets          = [],
    counter         = 1,
    limitCounter    = 0,
    prevLimit       = 0,
    prevDump        = process.hrtime();

var blankLine   = String.fromCharCode(0x20) + String.fromCharCode(0x0A),
    blankBuff   = Buffer.from(blankLine, "utf8"),
    separator   = String.fromCharCode(0x0D) + String.fromCharCode(0x0A); // CR+LF

var attemptSalvage      = false,
    originalExpected    = null,
    originalObserved    = null,
    currentLength,
    chunks;

var netwInterface;

function startCrawler () {

    var d = require('domain').create();

    d.on('error', function (e) {
        log('[domain.onError] "error" event detected: ' + e);
        d.dispose();
    });

    d.on('dispose', function () {

        if (!restarting) {
            log('[domain.onDispose] Old domain disposed of, attempting to restart crawler in a new domain.');
            startCrawler();
        }
    });

    d.run(init);
}

function init () {

    // Patch the oAuth module to allow for custom network interfaces
    patchOauth();

    // Grab the network interface from the command line, if any specified
    
    var malformedIP = setNetworkInterface();
    
    if (malformedIP) {
        // if the specified IP is malformed, do not proceed to connect with the default interface
        return;
    };

    
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
    );

    request.end();


    //
    // Register event listeners
    //
    
    request.on('socket', function(socket) {

        // Emitted after a socket is assigned to this request. 
        // 'socket' is an instance of net.Socket

        socket.setKeepAlive(true);
        log("Socket set to keep-alive.");

        socket.on('end', function () {
            log('[socket.onEnd] "end" event detected - the other end of the socket sent a FIN packet.');
        });

        socket.on('close', function (hadError) {
            log('[socket.onClose] "close" event detected. Socket was closed' + (hadError ? '' : ' NOT') + ' due to a transmission error.');
        });

        socket.on('error', function (e) {
            log('[socket.onError] "error" event detected: ' + e);
        });

        socket.on('timeout', function () {
            // This is only to notify that the socket has been idle. The user must manually close the connection. 
            log('[socket.onTimeout] "timeout" event detected - the user must now manually close the connection.')
        });

    });


    request.on('response', function (response) {

        // Emitted when a response is received to this request.
        // 'response' is an instance of http.IncomingMessage

        // Report on new request
        log("Response status code: " + response.statusCode + " (" + http.STATUS_CODES[response.statusCode] + "). Response headers:");
        console.log(response.headers);
        console.log();
        
        
        // Register event listeners

        response.on('data', processResponseData);

        response.on('end', function () {
            log('[response.onEnd] "end" event detected - stream has received an EOF, and no more "data" events will happen.');
            restart();
        });

        response.on('close', function () {
            log('[response.onClose] "close" event detected - the underlying connection was terminated before "end" event was emitted.');
        });

        response.on('error', function (e) {
            log('[response.onError] "error" event detected: ' + e);
        })

    });


    request.on('error', function(e) {
        
        // Report error
        
        log('[request.onError] "error" event detected: ' + e);

        restart();
    });
    
}

function restart () {

    if (!restarting) {

        restarting = true;

        // Set a time-out, then attempt another request
            
        timeout += timeout;

        if (timeout > maxTimeout) { timeout = maxTimeout };

        log("Timeout set, attempting another request in " + (timeout / 1000).toFixed(0) + " seconds.");

        setTimeout(function() {

            restarting = false;
            
            log("Attempting another request now.");

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

    var separatorPos    = d.indexOf(separator),
        expectedLenStr  = d.toString("utf8", 0, separatorPos),
        expectedLength  = Number(expectedLenStr),
        observedLength  = d.length - (separatorPos + 2);

    var stringMessage,
        parsedMessage;

    // what to do with empty lines?
    if (d.equals(blankBuff)) {
        log("[info] Keep-alive");
        return;
    }

    if ((separatorPos === 2 || separatorPos === 4) && Number.isInteger(expectedLength) && (observedLength === expectedLength)) {

        // it's a complete message
        // console.log("* complete");

        try {

            stringMessage = d.toString("utf8", separatorPos + 2);
            parsedMessage = JSON.parse(stringMessage);

            if (parsedMessage.id) {
                
                // this is a tweet
                
                tweets.push(stringMessage);

                // console.log(counter);
                counter++;

            } else {

                if (parsedMessage.limit) {

                    limitCounter = parsedMessage.limit.track;
                    log("limit");

                } else {

                    log("[info] Other message: " + stringMessage);
                }
            }

        } catch (e) {

            log("[err] Complete message failed to parse: " + d.toString("utf8"));
        }

        // in messages similar to one below, separator (LF) is positioned at the end (hence, zero "observed length"), 
        // and the first characted is 0 (hence, the string parseInts to 0, implying zero "expected length"); since 
        // they match, this is interpreted as a complete message, causing salvage to break down

        // 00"}

        // 0,"h":544,"resize":"fit"}}}]}},"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[{"url":"https:\/\/t.co\/QT5UIVsYfN","expanded_url":"https:\/\/twitter.com\/i\/web\/status\/780595042360713217","display_url":"twitter.com\/i\/web\/status\/7\u2026","indices":[116,139]}],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"und","timestamp_ms":"1474943335510"}

    } else {

        // it's a chunk

        if (!attemptSalvage) {

            // it's the first chunk

            if (Number.isInteger(expectedLength)) {

                originalExpected = expectedLength;
                currentLength = observedLength;

                attemptSalvage = true;
                
                chunks = [d.slice(separatorPos + 2)];

            } else {

                log("[err] Random chunk outside of salvage process: " + d.toString("utf8"));
            }
            

        } else {

            // it's a follow-up chunk

            currentLength += d.length;

            if (currentLength < originalExpected) {

                // we might be a few chunks short - continue with the salvage attempt
                chunks.push(d);

            } else if (currentLength === originalExpected) {

                // this might be the complete message

                chunks.push(d);

                // merge chunks
                var mendedMessage = "";

                for (i = 0; i < chunks.length; i++) {
                    mendedMessage += chunks[i].toString("utf8");
                }

                try {

                    parsedMessage = JSON.parse(mendedMessage);

                    if (parsedMessage.id) {
                        
                        // this is a tweet
                        
                        tweets.push(mendedMessage);

                        // console.log(counter);
                        counter++;

                        // console.log("* mended " + chunks.length);

                    } else {

                        if (parsedMessage.limit) {

                            limitCounter = parsedMessage.limit.track;
                            log("limit");

                        } else {

                            log("[info] Other mended message: " + stringMessage);
                        }

                        // log("[err] Mended message is not a tweet: " + mendedMessage);
                    }

                } catch (e) {

                    log("[err] Mended message failed to parse: " + mendedMessage);
                }

                attemptSalvage = false;

            } else if (currentLength > originalExpected) {

                // we've got extra pieces in the salvage buffer - abort the salvage attempt

                // debug ///////////////////////////////////////////////////////////////////////////////////////
                
                console.error("Too long, broken message below:");

                chunks.push(d);

                for (i = 0; i < chunks.length; i++) {
                    console.error(chunks[i].toString("utf8"));
                    console.error("xxx");
                }

                console.error("-------------------------------");

                // end debug ///////////////////////////////////////////////////////////////////////////////////
                
                log("[err] Too long, failed to de-chunk")
                attemptSalvage = false;

                // see if this is the start of a new message
                processResponseData(d);
            }
        }

        // end of de-chunking logic
    } 
   
    // end of processing function
}

function generateTimestamp() {
    
    // Generate timestamp in the form of "Tue Sep 11 2012 12:54:00 "
    
    var timeStamp = new Date(),
        timeString = timeStamp.toDateString() + " " + timeStamp.toLocaleTimeString() + " ";
    
    return timeString;
}

function log(message) {

    // console.log();
    console.log(generateTimestamp() + "- " + message);
    // console.log();

}

function setNetworkInterface() {
    
    // netwInterface = "10.215.96.235";
    // netwInterface = "147.26.184.73";

    var malformedIP = false;

    var optionDefinitions = [
        { name: "netwInterface", type: String }
    ];

    // var comLineOptions  = commandLineParser(optionDefinitions),
    var customInterface = comLineOptions.netwInterface,
        ipRegExp        = /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;

    if (customInterface && ipRegExp.test(customInterface)) {

        // log("valid ip address");
        netwInterface = customInterface;

    } else {

        log("Invalid IP address passed as a command line parameter");
        malformedIP = true;
    }

    return malformedIP;
}

function patchOauth() {

    // override original _createClient function to allow for custom network interfaces

    OAuth.prototype._createClient = function( port, hostname, method, path, headers, sslEnabled ) {

        var options = {
            host: hostname,
            port: port,
            path: path,
            method: method,
            headers: headers
        };

        // patch - checks for a global variable ////////////////////
        if (netwInterface) {
            options.localAddress = netwInterface;
        }
        // /////////////////////////////////////////////////////////

        var httpModel;

        if( sslEnabled ) {
            httpModel= https;
        } else {
            httpModel= http;
        }

        return httpModel.request(options);
    }
}




//
// Start the tweet collector
//
    
setInterval(function() {

    var tweetCount = tweets.length;
    
    var timeStamp = new Date();
    // var timeString = timeStamp.toDateString() + " " + timeStamp.toLocaleTimeString();

    if (tweetCount > 100) {

        // bundle tweets into a single string

        var tweetList = tweets.splice(0, tweetCount);

        var tweetString = tweetList.reduce(function(previousValue, currentValue) {
            return previousValue.toString() + ",\n" + currentValue.toString();
        });

        tweetString += ",\n";


        // dump tweets

        var year        = timeStamp.getUTCFullYear(),
            month       = (timeStamp.getUTCMonth() < 9) ? "0" + (timeStamp.getUTCMonth() + 1) : (timeStamp.getUTCMonth() + 1),
            day         = (timeStamp.getUTCDate() < 10) ? "0" + timeStamp.getUTCDate() : timeStamp.getUTCDate(),
            fileName    = "distributedReader.2.2." + readerOptions.name + "." + year + "." + month + "." + day + ".out";

        fs.appendFileSync(fileName, tweetString);


        // report the dump

        var tweetLost   = (limitCounter - prevLimit),
            pctLost     = ((limitCounter - prevLimit)/tweetCount*100).toFixed(1),
            currDump    = process.hrtime(),
            timePassed  = currDump[0] - prevDump[0] + (currDump[1] - prevDump[1])/1e9,
            twRate      = (tweetCount / timePassed).toFixed(2);

        log(tweetCount + " tweets dumped, " + tweetLost + " (" + pctLost + "%) throttled at " + twRate + " tw/sec");
        prevLimit = limitCounter;
        prevDump = currDump;


        // reset the timeout, if restarted recently

        if (timeout != defaultTimeout) {

            timeout = defaultTimeout;

            log("Timeout reset to " + (timeout / 1000).toFixed(0) + " seconds.");
        }

    } else {

        log("...");
    }

}, 10000);

startCrawler();