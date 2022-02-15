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

package io.siddhi.extension.map.json.sourcemapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.SubscriberUnAvailableException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonSourceMapperTestCase {
    private static final Logger log = (Logger) LogManager.getLogger(JsonSourceMapperTestCase.class);
    private final int waitTime = 2000;
    private final int timeout = 30000;
    private AtomicInteger count = new AtomicInteger();
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    @Test
    public void jsonSourceMapperTest1() throws Exception {
        log.info("test JsonSourceMapper 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(55.678f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(55f, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("WSO2@#$%^*", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.678,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest2() throws Exception {
        log.info("test JsonSourceMapper 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='true'))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(null, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":null\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":null,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":null,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest3() throws Exception {
        log.info("test JsonSourceMapper 3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='false'))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(null, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(null, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(56.0f, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":null\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":null,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":null,\n" +
                "         \"price\":56,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"price\":56,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":57.6,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 5, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest4() throws Exception {
        log.info("test JsonSourceMapper 4");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='true'))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("123", event.getData(0));
                            AssertJUnit.assertEquals(1.0f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(56.0f, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\": \"\"\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\": \"\",\n" +
                "         \"volume\": 100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\" \",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\": 100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\": 123,\n" +
                "         \"price\":1,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":10.234,\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":a\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":USD55.6,\n" +
                "         \"volume\": 100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\": 100.20\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest5() throws Exception {
        log.info("test JsonSourceMapper 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='true'))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "  {\n" +
                "   \"event\":{\n" +
                "       <symbol>WSO2</symbol>\n" +
                "       <price>55.6</price>\n" +
                "       <volume>100</volume>\n" +
                "   }\n" +
                "}");
        InMemoryBroker.publish("stock", " {\n" +
                "   \"event\":{\n" +
                "       \"TESTevent\": {\n" +
                "       \"symbol\": \"WSO2\",\n" +
                "       \"price\": \"55.6\",\n" +
                "       \"volume\": \"100\"\n" +
                "   }\n" +
                "}");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\": 100\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest6() throws Exception {
        log.info("test JsonSourceMapper 6");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='true'))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "   \"event\": {\n" +
                "       \"symbol\": \"WSO2\",\n" +
                "       \"price\": 55.6,\n" +
                "       \"volume\": [\n" +
                "           {\"v\": 100},\n" +
                "           {\"v\": 200}\n" +
                "       ]\n" +
                "   }\n" +
                "}");
        InMemoryBroker.publish("stock", " {\n" +
                "   \"event\":{\n" +
                "       \"symbol\":\"WSO2\",\n" +
                "       \"volume\":100\n" +
                "   }\n" +
                "}");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":10.12,\n" +
                "          \"discount\":\"3%\",\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\": 100\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest8() throws Exception {
        log.info("test JsonSourceMapper 8");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='true'))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"testEvent\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\": 100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\": 100\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest9() throws Exception {
        log.info("test JsonSourceMapper 9");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='true', " +
                "@attributes(symbol = \"event.symbol\", price = \"event.price\", " +
                "volume = \"event.volume\") " +
                "))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(52.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("WSO2", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(80L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(58.6f, event.getData(1));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(60.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " \n" +
                "[\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":52.6,\"volume\":100}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":53.6,\"volume\":99}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":54.6,\"volume\":80}}\n" +
                "]\n");
        InMemoryBroker.publish("stock", " \n" +
                "[\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}},\n" +
                "{\"testEvent\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":99}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":57.6,\"volume\":80}}\n" +
                "]\n");
        InMemoryBroker.publish("stock", "\n" +
                "[\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":58.6,\"volume\":100}},\n" +
                "{\"Event\":{\"symbol\":\"WSO2\",\"price\":59.6,\"volume\":99}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":60.6,\"volume\":80}}\n" +
                "]\n");
        SiddhiTestHelper.waitForEvents(waitTime, 7, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

   /*
   * Test cases for custom input mapping
   */

    @Test
    public void jsonSourceMapperTest10() throws Exception {
        log.info("test JsonSourceMapper 10");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='true', " +
                "@attributes(symbol = \"event.symbol\", price = \"event.price\", " +
                "volume = \"event.volume\") " +
                "))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(52.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(53.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(54.6f, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(58.6f, event.getData(1));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(60.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " \n" +
                "[\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":52.6,\"volume\":100}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":53.6,\"volume\":99}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":54.6,\"volume\":80}}\n" +
                "]\n");
        InMemoryBroker.publish("stock", " \n" +
                "[\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}},\n" +
                "{\"testEvent\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":99}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":57.6,\"volume\":80}}\n" +
                "]\n");
        InMemoryBroker.publish("stock", "\n" +
                "[\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":58.6,\"volume\":100}},\n" +
                "{\"Event\":{\"symbol\":\"WSO2\",\"price\":59.6,\"volume\":99}},\n" +
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":60.6,\"volume\":80}}\n" +
                "]\n");
        SiddhiTestHelper.waitForEvents(waitTime, 7, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest11() throws Exception {
        log.info("test JsonSourceMapper 11");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', enclosing.element=\"portfolio\", " +
                "@attributes(symbol = \"stock.company.symbol\", price = \"stock.price\", " +
                "volume = \"stock.volume\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}" +
                "}");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":56.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"wso2\"},\"price\":57.6}}" +
                "   ]\n" +
                "}\n");
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest12() throws Exception {
        log.info("test JsonSourceMapper 12");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', enclosing.element=\"portfolio\", " +
                "fail.on.missing.attribute=\"true\", " +
                "@attributes(symbol = \"stock.company.symbol\", price = \"stock.price\", " +
                "volume = \"stock.volume\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(null, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(76.6f, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(77.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}," +
                "       {\"stock\":{\"volume\":null,\"company\":{\"symbol\":\"wso2\"},\"price\":56.6}}" +
                "   ]\n" +
                "}\n");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"}}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"wso2\"},\"price\":null}}" +
                "   ]\n" +
                "}\n");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":76.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":null},\"price\":77.6}}" +
                "   ]\n" +
                "}\n");
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 5, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest13() throws Exception {
        log.info("test JsonSourceMapper 13");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', enclosing.element=\"portfolio\", " +
                "fail.on.missing.attribute=\"false\", " +
                "@attributes(symbol = \"stock.company.symbol\", price = \"stock.price\", " +
                "volume = \"stock.volume\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(200L, event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("wso2", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(null, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}," +
                "       {\"stock\":{\"volume\":null,\"company\":{\"symbol\":\"IBM\"},\"price\":56.6}}" +
                "   ]\n" +
                "}\n");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":66.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"IBM\"},\"price\":null}}" +
                "   ]\n" +
                "}\n");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":76.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":null},\"price\":77.6}}" +
                "   ]\n" +
                "}\n");
        SiddhiTestHelper.waitForEvents(waitTime, 6, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 6, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest14() throws Exception {
        log.info("test JsonSourceMapper with test multiple event");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(32.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "[{\n"
                + "  \"event\":{\n"
                + "      \"symbol\":\"WSO2\",\n"
                + "      \"price\":55.6,\n"
                + "      \"volume\":100}},"
                + "{\n"
                + "\"event\":{\n"
                + "\"symbol\":\"IBM\",\n"
                + "\"price\":32.6,\n"
                + "\"volume\":160\n"
                + "}\n"
                + "}]");

        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest15() throws Exception {
        log.info("test JsonSourceMapper with test json missing attribute");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "12");
        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("Json message 12 contains missing attributes"));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test
    public void jsonSourceMapperTest16() throws Exception {
        log.info("test JsonSourceMapper with test validate event identifier");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock",
                "{\"event1\":{\"symbol\":\"WSO2\",\"price\":52.6,\"volume\":100}}");
        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("Stream \"FooStream\" " +
                "does not have an attribute named \"event1\""));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test
    public void jsonSourceMapperTest17() throws Exception {
        log.info("test JsonSourceMapper with value's type");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol String, price double, volume int); " +
                "define stream BarStream (symbol String, price double, volume int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6d, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(55.678d, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(55d, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("WSO2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.678,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.0,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.0,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest18() throws Exception {
        log.info("test JsonSourceMapper with test values type's double");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol String, price double, volume int); " +
                "define stream BarStream (symbol String, price double, volume int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("contains incompatible attribute types and values"));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test
    public void jsonSourceMapperTest19() throws Exception {
        log.info("test JsonSourceMapper with test values type's Int");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol String, price double, volume int); " +
                "define stream BarStream (symbol String, price double, volume int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"true\",\n" +
                "         \"price\":55.0,\n" +
                "         \"volume\":100.0\n" +
                "      }\n" +
                " }");
        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("contains incompatible attribute types and values"));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test
    public void jsonSourceMapper20() throws Exception {
        log.info("test JsonSourceMapper with test Test values type boolean");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol Bool, price double, volume int); " +
                "define stream BarStream (symbol Bool, price double, volume int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"true\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100.0\n" +
                "      }\n" +
                " }");
        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("contains incompatible attribute types and values"));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test
    public void jsonSourceMapperTest21() throws Exception {
        log.info("test JsonSourceMapper with test json object type");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol Bool, price double, volume int); " +
                "define stream BarStream (symbol Bool, price double, volume int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", 12);
        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("Invalid JSON object received. Expected String"));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test
    public void jsonSourceMapperTest22() throws Exception {
        log.info("test JsonSourceMapper 22");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', enclosing.element=\"portfolio\", " +
                "fail.on.missing.attribute=\"false\", " +
                "@attributes(symbol = \"stock.company.symbol\", price = \"stock.price\", " +
                "volume = \"stock.volume\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(200L, event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("wso2", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(null, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", ("\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}," +
                "       {\"stock\":{\"volume\":null,\"company\":{\"symbol\":\"IBM\"},\"price\":56.6}}" +
                "   ]\n" +
                "}\n").getBytes(StandardCharsets.UTF_8));
        InMemoryBroker.publish("stock", ("\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":66.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"IBM\"},\"price\":null}}" +
                "   ]\n" +
                "}\n").getBytes(StandardCharsets.UTF_8));
        InMemoryBroker.publish("stock", ("\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":76.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":null},\"price\":77.6}}" +
                "   ]\n" +
                "}\n").getBytes(StandardCharsets.UTF_8));
        SiddhiTestHelper.waitForEvents(waitTime, 6, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 6, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest23() throws Exception {
        log.info("test JsonSourceMapper 23");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', enclosing.element=\"portfolio\", " +
                "@attributes(\"stock.company.symbol\", \"stock.price\", " +
                "\"stock.volume\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}" +
                "}");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":56.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"wso2\"},\"price\":57.6}}" +
                "   ]\n" +
                "}\n");
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest24() throws Exception {
        log.info("test JsonSourceMapper 24");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='stock', prop1='test', prop2='bar', " +
                "@map(type='json', enclosing.element=\"portfolio\", " +
                "@attributes(\"stock.company.symbol\", \"stock.price\", " +
                "\"trp:symbol\"))) " +
                "define stream FooStream (symbol string, price float, volume string); " +
                "define stream BarStream (symbol string, price float, volume string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    Assert.assertEquals("test", event.getData(2));
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}" +
                "}");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":56.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"wso2\"},\"price\":57.6}}" +
                "   ]\n" +
                "}\n");
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest25() throws Exception {
        log.info("test JsonSourceMapper 25");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='stock', prop1='test', prop2='bar', " +
                "@map(type='json', enclosing.element=\"portfolio\", " +
                "@attributes(symbol=\"stock.company.symbol\", price=\"stock.price\", " +
                "volume=\"trp:symbol\"))) " +
                "define stream FooStream (symbol string, price float, volume string); " +
                "define stream BarStream (symbol string, price float, volume string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    Assert.assertEquals("test", event.getData(2));
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}" +
                "}");
        InMemoryBroker.publish("stock", "\n" +
                "{\"portfolio\":\n" +
                "   [" +
                "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":56.6}}," +
                "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"wso2\"},\"price\":57.6}}" +
                "   ]\n" +
                "}\n");
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest26() throws Exception {
        log.info("test JsonSourceMapper 26");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(55.678f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(55f, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("WSO2@#$%^*", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", "{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":100\n" +
                "      }");
        InMemoryBroker.publish("stock", "{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.678,\n" +
                "         \"volume\":100\n" +
                "      }");
        InMemoryBroker.publish("stock", "{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }");
        InMemoryBroker.publish("stock", "{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }");
        InMemoryBroker.publish("stock", "{\"symbol\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }}");
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest27() throws Exception {
        log.info("test JsonSourceMapper 27");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', " +
                "@map(type='json', fail.on.missing.attribute='false'))\n" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(52.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("WSO2", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(80L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(58.6f, event.getData(1));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(59.6f, event.getData(1));
                            break;
                        case 8:
                            AssertJUnit.assertEquals(60.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " \n" +
                "[\n" +
                "{\"symbol\":\"WSO2\",\"price\":52.6,\"volume\":100},\n" +
                "{\"symbol\":\"WSO2\",\"price\":53.6,\"volume\":99},\n" +
                "{\"symbol\":\"WSO2\",\"price\":54.6,\"volume\":80}\n" +
                "]\n");
        InMemoryBroker.publish("stock", " \n" +
                "[\n" +
                "{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100},\n" +
                "{\"testEvent\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":99}},\n" +
                "{\"symbol\":\"WSO2\",\"price\":57.6,\"volume\":80}\n" +
                "]\n");
        InMemoryBroker.publish("stock", "\n" +
                "[\n" +
                "{\"symbol\":\"WSO2\",\"price\":58.6,\"volume\":100},\n" +
                "{\"symbol\":\"WSO2\",\"price\":59.6,\"volume\":99},\n" +
                "{\"symbol\":\"WSO2\",\"price\":60.6,\"volume\":80}\n" +
                "]\n");
        SiddhiTestHelper.waitForEvents(waitTime, 8, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest28() throws Exception {
        log.info("test JsonSourceMapper 28");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol string, price object, volume long); " +
                "define stream BarStream (symbol string, price object, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        Gson gson = new Gson();
        Object jsonObject1 = objectMapper.readValue("[25,25]", Object.class);
        Object jsonObject2 = objectMapper.readValue("{\"lkr\":55.678}", Object.class);
        Object jsonObject3 = objectMapper.readValue("{\"prices\":25, \"priceArray\":[25,25]}", Object.class);
        Object jsonObject4 = objectMapper.readValue("{\"price\":25, \"priceArray\":[25,25,25], " +
                "\"priceObject\":{\"price\":25,\"amount\":5,\"items\":[1,2,3]}}", Object.class);
        Object jsonObject5 = objectMapper.readValue("{\"price\":25, \"priceArray\":[25,25,25], " +
                "\"priceObject\":{\"price\":25,\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}}", Object
                .class);
        Object jsonObject6 = objectMapper.readValue("{\"price\":25, \"priceArray\":[{\"lkr\":25},{\"lkr\":25}," +
                "{\"lkr\":25}], " +
                "\"priceObject\":{\"price\":25,\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}}", Object
                .class);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(jsonObject1, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(jsonObject2, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(jsonObject3, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(jsonObject4, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(jsonObject5, event.getData(1));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(jsonObject6, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":[25,25],\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":{\"lkr\":55.678},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":{\"prices\":25, \"priceArray\":[25,25]},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":{\"price\":25, \"priceArray\":[25,25,25], \"priceObject\":{\"price\":25," +
                "\"amount\":5,\"items\":[1,2,3]}},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":{\"price\":25, \"priceArray\":[25,25,25], \"priceObject\":{\"price\":25," +
                "\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":{\"price\":25, \"priceArray\":[{\"lkr\":25},{\"lkr\":25},{\"lkr\":25}], " +
                "\"priceObject\":{\"price\":25," +
                "\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");

        SiddhiTestHelper.waitForEvents(waitTime, 6, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 6, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest29() throws Exception {
        log.info("test JsonSourceMapper 29");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='json')) " +
                "define stream FooStream (symbol object, price string, volume long); " +
                "define stream BarStream (symbol object, price string, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals("[25,25]", event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("{\"lkr\":55.678}", event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("{\"prices\":25,\"priceArray\":[25,25]}", event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("{\"price\":25,\"priceArray\":[25,25,25],\"priceObject\":" +
                                    "{\"price\":25,\"amount\":5,\"items\":[1,2,3]}}", event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("{\"price\":25,\"priceArray\":[25,25,25],\"priceObject\":" +
                                            "{\"price\":25,\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}}",
                                    event.getData(1));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("{\"price\":25,\"priceArray\":[{\"lkr\":25},{\"lkr\":25}," +
                                    "{\"lkr\":25}],\"priceObject\":{\"price\":25,\"amount\":5,\"items\":[{\"id\":1}," +
                                    "{\"id\":2},{\"id\":3}]}}", event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":[25,25],\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":{\"lkr\":55.678},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":{\"prices\":25, \"priceArray\":[25,25]},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":{\"price\":25, \"priceArray\":[25,25,25], \"priceObject\":{\"price\":25," +
                "\"amount\":5,\"items\":[1,2,3]}},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":{\"price\":25, \"priceArray\":[25,25,25], \"priceObject\":{\"price\":25," +
                "\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":{\"price\":25,\"priceArray\":[{\"lkr\":25},{\"lkr\":25},{\"lkr\":25}], " +
                "\"priceObject\":{\"price\":25," +
                "\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}},\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
//        InMemoryBroker.publish("stock", " {\n" +
//                "      \"event\":{\n" +
//                "         \"symbol\":25,\n" +
//                "         \"price\":{\"price\":25,\"priceArray\":[{\"lkr\":25},{\"lkr\":25},{\"lkr\":25}], " +
//                "\"priceObject\":{\"price\":25," +
//                "\"amount\":5,\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}},\n" +
//                "         \"volume\":100\n" +
//                "      }\n" +
//                " }");

        SiddhiTestHelper.waitForEvents(waitTime, 6, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 6, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest30() {
        log.info("test JsonSourceMapper 30: event.grouping.enabled=false");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='stock', prop1='test', prop2='bar', " +
                "@map(type='json', enclosing.element=\"portfolio\", event.grouping.enabled='false', " +
                "@attributes(symbol=\"stock.company.symbol\", price=\"stock.price\", " +
                "volume=\"trp:symbol\"))) " +
                "define stream FooStream (symbol string, price float, volume string); " +
                "define stream BarStream (symbol string, price float, volume string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertEquals(events.length, 1,
                        "An array of events received when event.grouping.enabled is false!");
                for (Event event : events) {
                    Assert.assertEquals("test", event.getData(2));
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        try {
            InMemoryBroker.publish("stock", "\n" +
                    "{\"portfolio\":\n" +
                    "   {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":55.6}}" +
                    "}");
            InMemoryBroker.publish("stock", "\n" +
                    "{\"portfolio\":\n" +
                    "   [" +
                    "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":56.6}}," +
                    "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"wso2\"},\"price\":57.6}}" +
                    "   ]\n" +
                    "}\n");
        } catch (SubscriberUnAvailableException e) {
            AssertJUnit.fail("Could not publish message. " + e.getStackTrace());
        }
        try {
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Interrupted while waiting for event arrival " + e.getStackTrace());
        }
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest31() {
        log.info("test JsonSourceMapper 31: event.grouping.enabled=true");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='stock', prop1='test', prop2='bar', " +
                "@map(type='json', enclosing.element=\"portfolio\", event.grouping.enabled='true', " +
                "@attributes(symbol=\"stock.company.symbol\", price=\"stock.price\", " +
                "volume=\"trp:symbol\"))) " +
                "define stream FooStream (symbol string, price float, volume string); " +
                "define stream BarStream (symbol string, price float, volume string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertEquals(events.length, 2,
                        "An array of events is expected when event.grouping.enabled is true!");
                for (Event event : events) {
                    Assert.assertEquals("test", event.getData(2));
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(56.6f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        try {
            InMemoryBroker.publish("stock", "\n" +
                    "{\"portfolio\":\n" +
                    "   [" +
                    "       {\"stock\":{\"volume\":100,\"company\":{\"symbol\":\"wso2\"},\"price\":56.6}}," +
                    "       {\"stock\":{\"volume\":200,\"company\":{\"symbol\":\"wso2\"},\"price\":57.6}}" +
                    "   ]\n" +
                    "}\n");
        } catch (SubscriberUnAvailableException e) {
            AssertJUnit.fail("Could not publish message. " + e.getStackTrace());
        }
        try {
            SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Interrupted while waiting for event arrival " + e.getStackTrace());
        }
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }
}
