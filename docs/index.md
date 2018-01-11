siddhi-map-json
======================================

The **siddhi-map-json extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> 
which is used to convert JSON message to/from Siddhi events.  

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-json">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-json/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-json/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json/api/4.0.17">4.0.17</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this 
extension you can replace the component <a target="_blank" href="https://github
.com/wso2-extensions/siddhi-map-json/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.map.json</groupId>
        <artifactId>siddhi-map-json</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Stat This repository can be independently released from Siddhi.us |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-json/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-json/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json/api/4.0.17/#json-sink-mapper">json</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink-mapper">(Sink Mapper)</a>)*<br><div style="padding-left: 1em;"><p>Event to JSON output mapper. <br>Transports which publish  messages can utilize this extensionto convert the Siddhi event to JSON message. <br>Users can either send a pre-defined JSON format or a custom JSON message.<br></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json/api/4.0.17/#json-source-mapper">json</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source-mapper">(Source Mapper)</a>)*<br><div style="padding-left: 1em;"><p>JSON to Event input mapper. Transports which accepts JSON messages can utilize this extensionto convert the incoming JSON message to Siddhi event. Users can either send a pre-defined JSON format where event conversion will happen without any configs or can use json path to map from a custom JSON message.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-json/issues">GitHub 
  Issue
   Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github
  .com/wso2-extensions/siddhi-map-json/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
