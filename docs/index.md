Siddhi Map JSON
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-json/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-json/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-map-json.svg)](https://github.com/siddhi-io/siddhi-map-json/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-map-json.svg)](https://github.com/siddhi-io/siddhi-map-json/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-map-json.svg)](https://github.com/siddhi-io/siddhi-map-json/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-map-json.svg)](https://github.com/siddhi-io/siddhi-map-json/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-map-json extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that converts JSON messages to/from Siddhi events.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 5.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.map.json/siddhi-map-json/">here</a>.
* Versions 4.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.map.json/siddhi-map-json">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-json/api/5.1.0">5.1.0</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-json/api/5.1.0/#json-sink-mapper">json</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">Sink Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension is an Event to JSON output mapper. <br>Transports that publish  messages can utilize this extension to convert Siddhi events to JSON messages. <br>You can either send a pre-defined JSON format or a custom JSON message.<br></p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-json/api/5.1.0/#json-source-mapper">json</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">Source Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension is a JSON-to-Event input mapper. Transports that accept JSON messages can utilize this extension to convert an incoming JSON message into a Siddhi event. Users can either send a pre-defined JSON format, where event conversion happens without any configurations, or use the JSON path to map from a custom JSON message.<br>In default mapping, the JSON string of the event can be enclosed by the element "event", though optional.</p></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

