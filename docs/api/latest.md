# API Docs - v4.0.29-SNAPSHOT

## Sinkmapper

### json *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink-mapper">(Sink Mapper)</a>*

<p style="word-wrap: break-word">This extension is an Event to JSON output mapper. <br>Transports that publish  messages can utilize this extensionto convert Siddhi events to JSON messages. <br>You can either send a pre-defined JSON format or a custom JSON message.<br></p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(..., @map(type="json", validate.json="<BOOL>", enclosing.element="<STRING>")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">validate.json</td>
        <td style="vertical-align: top; word-wrap: break-word">If this property is set to 'true', it enables JSON validation for the JSON messages generated. <br>When validation is carried out, messages that do not adhere to proper JSON standards are dropped. This property is set to 'false' by default. <br></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">enclosing.element</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies the enclosing element to be used if multiple events are sent in the same JSON message. <br>WSO2 SP treats the child elements of the given enclosing element as events and executes JSON expressions on them. <br>If an enclosing.element is not provided, the multiple event scenario is disregarded and JSON path is evaluated based on the root element.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='inMemory', topic='stock', @map(type='json'))
define stream FooStream (symbol string, price float, volume long);

```
<p style="word-wrap: break-word">Above configuration does a default JSON input mapping that generates the output given below.<br>{<br>&nbsp;&nbsp;&nbsp;&nbsp;"event":{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"symbol":WSO2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"price":55.6,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"volume":100<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>}<br></p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@sink(type='inMemory', topic='{{symbol}}', @map(type='json', enclosing.element='$.portfolio', validate.json='true', @payload( """{"StockData":{"Symbol":"{{symbol}}","Price":{{price}}}""")))
define stream BarStream (symbol string, price float, volume long);
```
<p style="word-wrap: break-word">The above configuration performs a custom JSON mapping that generates the following JSON message as the output.<br>{"portfolio":{<br>&nbsp;&nbsp;&nbsp;&nbsp;"StockData":{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Symbol":WSO2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Price":55.6<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</p>

## Sourcemapper

### json *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source-mapper">(Source Mapper)</a>*

<p style="word-wrap: break-word">This extension is a JSON to Event input mapper. Transports that accept JSON messages canutilize this extension to convert the incoming JSON messages to Siddhi events. You can either senda pre-defined JSON format where the event conversion is carried out without any configurations,or use a JSON path to map from a custom JSON message.<br>In default mapping, the JSON string of the event can be optionally enclosed by the "event" element.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(..., @map(type="json", enclosing.element="<STRING>", fail.on.missing.attribute="<BOOL>")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">enclosing.element</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies the enclosing element to use if multiple events are received in the same JSON message. <br>WSO2 SP considers the child elements of the given enclosing element as events and executes JSON path expressions on them. <br>If an enclosing element is not provided, the multiple event scenario is disregarded and the JSON path is evaluated based on the root element.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">fail.on.missing.attribute</td>
        <td style="vertical-align: top; word-wrap: break-word">If this value is set to 'true', events are dropped when a JSON execution fails or returns a 'null' value. If this property is set to 'false', events with 'null' values are sent to Siddhi where you can handle it as required (e.g., by assigning a default value).<br></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='inMemory', topic='stock', @map(type='json'))
define stream FooStream (symbol string, price float, volume long);

```
<p style="word-wrap: break-word">The above configuration performs a default JSON input mapping.<br>&nbsp;For a single event, the expected input should be in one of the following formats.<br>{<br>&nbsp;&nbsp;&nbsp;&nbsp;"event":{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"symbol":"WSO2",<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"price":55.6,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"volume":100<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>}<br><br>or <br><br>{<br>&nbsp;&nbsp;&nbsp;&nbsp;"symbol":"WSO2",<br>&nbsp;&nbsp;&nbsp;&nbsp;"price":55.6,<br>&nbsp;&nbsp;&nbsp;&nbsp;"volume":100<br>}<br></p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@source(type='inMemory', topic='stock', @map(type='json'))
define stream FooStream (symbol string, price float, volume long);

```
<p style="word-wrap: break-word">The above configuration performs a default JSON input mapping. <br>For multiple events, the expected input should be in one of the following formats.<br>[<br>{"event":{"symbol":"WSO2","price":55.6,"volume":100}},<br>{"event":{"symbol":"WSO2","price":56.6,"volume":99}},<br>{"event":{"symbol":"WSO2","price":57.6,"volume":80}}<br>]<br><br>or <br><br>[<br>{"symbol":"WSO2","price":55.6,"volume":100},<br>{"symbol":"WSO2","price":56.6,"volume":99},<br>{"symbol":"WSO2","price":57.6,"volume":80}<br>]</p>

<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@source(type='inMemory', topic='stock', @map(type='json', enclosing.element="$.portfolio", @attributes(symbol = "company.symbol", price = "price", volume = "volume")))
```
<p style="word-wrap: break-word">The above configuration performs a custom JSON mapping.<br>For a single event, the expected input is as follows.<br>.{<br>&nbsp;"portfolio":{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"stock":{        "volume":100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"company":{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"symbol":"WSO2"<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"price":55.6<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>}<br></p>

<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
@source(type='inMemory', topic='stock', @map(type='json', enclosing.element="$.portfolio", @attributes(symbol = "stock.company.symbol", price = "stock.price", volume = "stock.volume")))
define stream FooStream (symbol string, price float, volume long);

```
<p style="word-wrap: break-word">The above configuration performs a custom JSON mapping.<br>For multiple events, the expected input is as follows.<br>.{"portfolio":<br>&nbsp;&nbsp;&nbsp;[     {"stock":{"volume":100,"company":{"symbol":"wso2"},"price":56.6}},     {"stock":{"volume":200,"company":{"symbol":"wso2"},"price":57.6}}   ]<br>}<br></p>

