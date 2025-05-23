/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.map.json.sinkmapper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;


/**
 * Mapper class to convert a Siddhi message to a JSON message. User can provide a JSON template or else we will be
 * using a predefined JSON message format. In some instances
 * coding best practices have been compensated for performance concerns.
 */

@Extension(
        name = "json",
        namespace = "sinkMapper",
        description = "" +
                "This extension is an Event to JSON output mapper. \n" +
                "Transports that publish  messages can utilize this extension " +
                "to convert Siddhi events to " +
                "JSON messages. \n" +
                "You can either send a pre-defined JSON format or a custom JSON message.\n",
        parameters = {
                @Parameter(name = "validate.json",
                        description = "" +
                                "If this property is set to `true`, it enables JSON validation for the JSON messages" +
                                " generated. \n" +
                                "When validation is carried out, messages that do not adhere to proper JSON standards" +
                                " are dropped. This property is set to 'false' by default. \n",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(name = "enclosing.element",
                        description =
                                "This specifies the enclosing element to be used if multiple events are sent in the" +
                                        " same JSON message. \n" +
                                        "Siddhi treats the child elements of the given enclosing element as events"
                                        + " and executes JSON expressions on them. \nIf an `enclosing.element` "
                                        + "is not provided, the multiple event scenario is disregarded and JSON " +
                                        "path is evaluated based on the root element.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "$"),
                @Parameter(name = "event.grouping.enabled",
                        description = "This parameter is used to preserve event chunks when the value is set to " +
                                "'true' or the value can be set to 'false' to separate events",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
                @Parameter(name = "enable.null.attribute.value",
                        description = "If this parameter is true, output parameter values will contain null values " +
                                "if not they will be undefined",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false")
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='json'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "Above configuration does a default JSON input mapping that "
                                + "generates the output given below.\n"
                                + "{\n"
                                + "    \"event\":{\n"
                                + "        \"symbol\":WSO2,\n"
                                + "        \"price\":55.6,\n"
                                + "        \"volume\":100\n"
                                + "    }\n"
                                + "}\n"),
                @Example(
                        syntax = "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', enclosing"
                                + ".element='$.portfolio', validate.json='true', @payload( "
                                + "\"\"\"{\"StockData\":{\"Symbol\":\"{{symbol}}\",\"Price\":{{price}}}}\"\"\")))\n"
                                + "define stream BarStream (symbol string, price float, volume long);",
                        description = "The above configuration performs a custom JSON mapping that "
                                + "generates the following JSON message as the output.\n"
                                + "{"
                                + "\"portfolio\":{\n"
                                + "    \"StockData\":{\n"
                                + "        \"Symbol\":WSO2,\n"
                                + "        \"Price\":55.6\n"
                                + "      }\n"
                                + "  }\n"
                                + "}")
        }
)

public class JsonSinkMapper extends SinkMapper {
    private static final Logger log = LogManager.getLogger(JsonSinkMapper.class);
    private static final String EVENT_PARENT_TAG = "event";
    private static final String ENCLOSING_ELEMENT_IDENTIFIER = "enclosing.element";
    private static final String DEFAULT_ENCLOSING_ELEMENT = "$";
    private static final String JSON_VALIDATION_IDENTIFIER = "validate.json";
    private static final String EVENT_GROUPING_ENABLED = "event.grouping.enabled";
    private static final String ENABLE_NULL_ATTRIBUTE_VALUE = "enable.null.attribute.value";
    private static final String JSON_EVENT_SEPERATOR = ",";
    private static final String JSON_KEYVALUE_SEPERATOR = ":";
    private static final String JSON_ARRAY_START_SYMBOL = "[";
    private static final String JSON_ARRAY_END_SYMBOL = "]";
    private static final String JSON_EVENT_START_SYMBOL = "{";
    private static final String JSON_EVENT_END_SYMBOL = "}";
    private static final String UNDEFINED = "undefined";

    private String[] attributeNameArray;
    private String enclosingElement = null;
    private boolean isJsonValidationEnabled = false;
    private boolean eventGroupingEnabled = true;
    private boolean enableNullAttributeValue = false;


    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }


    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param streamDefinition          The stream definition
     * @param optionHolder              Option holder containing static and dynamic options
     * @param payloadTemplateBuilderMap Unmapped list of payloads for reference
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     Map<String, TemplateBuilder> payloadTemplateBuilderMap, ConfigReader mapperConfigReader,
                     SiddhiAppContext siddhiAppContext) {

        this.attributeNameArray = streamDefinition.getAttributeNameArray();
        this.enclosingElement = optionHolder.validateAndGetStaticValue(ENCLOSING_ELEMENT_IDENTIFIER, null);
        this.isJsonValidationEnabled = Boolean.parseBoolean(optionHolder
                .validateAndGetStaticValue(JSON_VALIDATION_IDENTIFIER, "false"));
        this.eventGroupingEnabled = Boolean.parseBoolean(optionHolder
                .validateAndGetStaticValue(EVENT_GROUPING_ENABLED, "true"));
        this.enableNullAttributeValue = Boolean.parseBoolean(optionHolder
                .validateAndGetStaticValue(ENABLE_NULL_ATTRIBUTE_VALUE, "false"));

        //if @payload() is added there must be at least 1 element in it, otherwise a SiddhiParserException raised
        if (payloadTemplateBuilderMap != null && payloadTemplateBuilderMap.size() != 1) {
            throw new SiddhiAppCreationException("Json sink-mapper does not support multiple @payload mappings, " +
                    "error at the mapper of '" + streamDefinition.getId() + "'");
        }
        if (payloadTemplateBuilderMap != null &&
                payloadTemplateBuilderMap.get(payloadTemplateBuilderMap.keySet().iterator().next()).isObjectMessage()) {
            throw new SiddhiAppCreationException("Json sink-mapper does not support object @payload mappings, " +
                    "error at the mapper of '" + streamDefinition.getId() + "'");
        }
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        if (eventGroupingEnabled) {
            StringBuilder sb = new StringBuilder();
            if (payloadTemplateBuilderMap == null) {
                String jsonString = constructJsonForDefaultMapping(events);
                sb.append(jsonString);
            } else {
                sb.append(constructJsonForCustomMapping(events,
                        payloadTemplateBuilderMap.get(payloadTemplateBuilderMap.keySet().iterator().next())));
            }
            if (!isJsonValidationEnabled) {
                sinkListener.publish(sb.toString());
            } else if (isValidJson(sb.toString())) {
                sinkListener.publish(sb.toString());
            } else {
                log.error("Invalid json string : {}. Hence dropping the message.", sb.toString());
            }
        } else {
            for (Event event : events) {
                mapAndSend(event, optionHolder, payloadTemplateBuilderMap, sinkListener);
            }
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        StringBuilder sb = null;
        if (payloadTemplateBuilderMap == null) {
            String jsonString = constructJsonForDefaultMapping(event);
            if (jsonString != null) {
                sb = new StringBuilder();
                sb.append(jsonString);
            }
        } else {
            sb = new StringBuilder();
            sb.append(constructJsonForCustomMapping(event,
                    payloadTemplateBuilderMap.get(payloadTemplateBuilderMap.keySet().iterator().next())));
        }
        if (sb != null) {
            if (!isJsonValidationEnabled) {
                sinkListener.publish(sb.toString());
            } else if (isValidJson(sb.toString())) {
                sinkListener.publish(sb.toString());
            } else {
                log.error("Invalid json string : {}. Hence dropping the message.", sb.toString());
            }
        }
    }

    private String constructJsonForDefaultMapping(Object eventObj) {
        StringBuilder sb = new StringBuilder();
        int numberOfOuterObjects;
        if (enclosingElement != null) {
            String[] nodeNames = enclosingElement.split("\\.");
            if (DEFAULT_ENCLOSING_ELEMENT.equals(nodeNames[0])) {
                numberOfOuterObjects = nodeNames.length - 1;
            } else {
                numberOfOuterObjects = nodeNames.length;
            }
            for (String nodeName : nodeNames) {
                if (!DEFAULT_ENCLOSING_ELEMENT.equals(nodeName)) {
                    sb.append(JSON_EVENT_START_SYMBOL).append("\"").append(nodeName).append("\"")
                            .append(JSON_KEYVALUE_SEPERATOR);
                }
            }
            if (eventObj instanceof Event) {
                Event event = (Event) eventObj;
                JsonObject jsonEvent;
                if (enableNullAttributeValue) {
                    jsonEvent = constructSingleEventForDefaultMapping(event);
                } else {
                    jsonEvent = constructSingleEventForDefaultMapping(doPartialProcessing(event));
                }

                sb.append(jsonEvent);
            } else if (eventObj instanceof Event[]) {
                JsonArray eventArray = new JsonArray();
                for (Event event : (Event[]) eventObj) {
                    if (enableNullAttributeValue) {
                        eventArray.add(constructSingleEventForDefaultMapping(event));
                    } else {
                        eventArray.add(constructSingleEventForDefaultMapping(doPartialProcessing(event)));
                    }

                }
                sb.append(eventArray.toString());
            } else {
                log.error(
                        "Invalid object type. {} cannot be converted to an event or event array. Hence dropping " +
                                "message.",
                        eventObj.toString());
                return null;
            }
            for (int i = 0; i < numberOfOuterObjects; i++) {
                sb.append(JSON_EVENT_END_SYMBOL);
            }
            return sb.toString();
        } else {
            if (eventObj instanceof Event) {
                Event event = (Event) eventObj;
                JsonObject jsonEvent;
                if (enableNullAttributeValue) {
                    jsonEvent = constructSingleEventForDefaultMapping(event);
                } else {
                    jsonEvent = constructSingleEventForDefaultMapping(doPartialProcessing(event));
                }

                return jsonEvent.toString();
            } else if (eventObj instanceof Event[]) {
                JsonArray eventArray = new JsonArray();
                for (Event event : (Event[]) eventObj) {
                    if (enableNullAttributeValue) {
                        eventArray.add(constructSingleEventForDefaultMapping(event));
                    } else {
                        eventArray.add(constructSingleEventForDefaultMapping(doPartialProcessing(event)));
                    }

                }
                return (eventArray.toString());
            } else {
                log.error("Invalid object type. {} cannot be converted to an event or event array.",
                        eventObj.toString());
                return null;
            }
        }
    }

    private String constructJsonForCustomMapping(Object eventObj, TemplateBuilder payloadTemplateBuilder) {
        StringBuilder sb = new StringBuilder();
        int numberOfOuterObjects = 0;
        if (enclosingElement != null) {
            String[] nodeNames = enclosingElement.split("\\.");
            if (DEFAULT_ENCLOSING_ELEMENT.equals(nodeNames[0])) {
                numberOfOuterObjects = nodeNames.length - 1;
            } else {
                numberOfOuterObjects = nodeNames.length;
            }
            for (String nodeName : nodeNames) {
                if (!DEFAULT_ENCLOSING_ELEMENT.equals(nodeName)) {
                    sb.append(JSON_EVENT_START_SYMBOL).append("\"").append(nodeName).append("\"")
                            .append(JSON_KEYVALUE_SEPERATOR);
                }
            }
            if (eventObj instanceof Event) {
                Event event;
                if (enableNullAttributeValue) {
                    event = (Event) eventObj;
                } else {
                    event = doPartialProcessing((Event) eventObj);
                }
                sb.append(payloadTemplateBuilder.build(event));
            } else if (eventObj instanceof Event[]) {
                String jsonEvent;
                sb.append(JSON_ARRAY_START_SYMBOL);
                for (Event e : (Event[]) eventObj) {
                    if (enableNullAttributeValue) {
                        jsonEvent = (String) payloadTemplateBuilder.build(e);
                    } else {
                        jsonEvent = (String) payloadTemplateBuilder.build(doPartialProcessing(e));
                    }

                    if (jsonEvent != null) {
                        sb.append(jsonEvent).append(JSON_EVENT_SEPERATOR).append("\n");
                    }
                }
                sb.delete(sb.length() - 2, sb.length());
                sb.append(JSON_ARRAY_END_SYMBOL);
            } else {
                log.error(
                        "Invalid object type. {} cannot be converted to an event or event array. Hence dropping " +
                                "message.",
                        eventObj.toString());
                return null;
            }
            for (int i = 0; i < numberOfOuterObjects; i++) {
                sb.append(JSON_EVENT_END_SYMBOL);
            }
            return sb.toString();
        } else {
            if (eventObj.getClass() == Event.class) {
                if (enableNullAttributeValue) {
                    return (String) payloadTemplateBuilder.build((Event) eventObj);
                } else {
                    return (String) payloadTemplateBuilder.build(doPartialProcessing((Event) eventObj));
                }
            } else if (eventObj.getClass() == Event[].class) {
                String jsonEvent;
                sb.append(JSON_ARRAY_START_SYMBOL);
                for (Event event : (Event[]) eventObj) {
                    if (enableNullAttributeValue) {
                        jsonEvent = (String) payloadTemplateBuilder.build(event);
                    } else {
                        jsonEvent = (String) payloadTemplateBuilder.build(doPartialProcessing(event));
                    }

                    if (jsonEvent != null) {
                        sb.append(jsonEvent).append(JSON_EVENT_SEPERATOR).append("\n");
                    }
                }
                sb.delete(sb.length() - 2, sb.length());
                sb.append(JSON_ARRAY_END_SYMBOL);
                return sb.toString();
            } else {
                log.error(
                        "Invalid object type. {} cannot be converted to an event or event array. Hence dropping " +
                                "message.",
                        eventObj.toString());
                return null;
            }
        }
    }

    private JsonObject constructSingleEventForDefaultMapping(Event event) {
        Object[] data = event.getData();
        JsonObject jsonEventObject = new JsonObject();
        JsonObject innerParentObject = new JsonObject();
        String attributeName;
        Object attributeValue;
        Gson gson;
        if (enableNullAttributeValue) {
            gson = new GsonBuilder()
                    .serializeNulls()
                    .setPrettyPrinting()
                    .create();
        } else {
            gson = new Gson();
        }

        for (int i = 0; i < data.length; i++) {
            attributeName = attributeNameArray[i];
            attributeValue = data[i];
            if (attributeValue != null) {
                if (attributeValue.getClass() == String.class) {
                    innerParentObject.addProperty(attributeName, attributeValue.toString());
                } else if (attributeValue instanceof Number) {
                    innerParentObject.addProperty(attributeName, (Number) attributeValue);
                } else if (attributeValue instanceof Boolean) {
                    innerParentObject.addProperty(attributeName, (Boolean) attributeValue);
                } else if (attributeValue instanceof Map) {
                    if (!((Map) attributeValue).isEmpty()) {
                        innerParentObject.add(attributeName, gson.toJsonTree(attributeValue));
                    }
                }
            } else {
                innerParentObject.add(attributeName, null);
            }
        }
        jsonEventObject.add(EVENT_PARENT_TAG, innerParentObject);
        return jsonEventObject;
    }

    private Event doPartialProcessing(Event event) {
        Object[] data = event.getData();
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                data[i] = UNDEFINED;
            }
        }
        return event;
    }

    private static boolean isValidJson(String jsonInString) {
        try {
            new Gson().fromJson(jsonInString, Object.class);
            return true;
        } catch (com.google.gson.JsonSyntaxException ex) {
            return false;
        }
    }
}
