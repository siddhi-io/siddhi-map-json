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

package org.wso2.extension.siddhi.map.json.sourcemapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.concurrent.atomic.AtomicInteger;

public class JsonSourceMapperTestCase {
    private static final Logger log = Logger.getLogger(JsonSourceMapperTestCase.class);
    private final int waitTime = 2000;
    private final int timeout = 30000;
    private AtomicInteger count = new AtomicInteger();

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    @Test
    public void jsonSourceMapperTest1() throws InterruptedException {
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
    public void jsonSourceMapperTest2() throws InterruptedException {
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
    public void jsonSourceMapperTest3() throws InterruptedException {
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
    public void jsonSourceMapperTest4() throws InterruptedException {
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
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest5() throws InterruptedException {
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
    public void jsonSourceMapperTest6() throws InterruptedException {
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
    public void jsonSourceMapperTest8() throws InterruptedException {
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
    public void jsonSourceMapperTest9() throws InterruptedException {
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
    public void jsonSourceMapperTest10() throws InterruptedException {
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
    public void jsonSourceMapperTest11() throws InterruptedException {
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
    public void jsonSourceMapperTest12() throws InterruptedException {
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
    public void jsonSourceMapperTest13() throws InterruptedException {
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
    public void jsonSourceMapperTest14() throws InterruptedException {
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
                +   "\"event\":{\n"
                +       "\"symbol\":\"IBM\",\n"
                +       "\"price\":32.6,\n"
                +       "\"volume\":160\n"
                +   "}\n"
                + "}]");

        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest15() throws InterruptedException {
        log.info("test JsonSourceMapper with test json missing attribute");
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

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest16() throws InterruptedException {
        log.info("test JsonSourceMapper with test validate event identifier");
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

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest17() throws InterruptedException {
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
    public void jsonSourceMapperTest18() throws InterruptedException {
        log.info("test JsonSourceMapper with test values type's double");
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
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest19() throws InterruptedException {
        log.info("test JsonSourceMapper with test values type's Int");
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
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapper20() throws InterruptedException {
        log.info("test JsonSourceMapper with test Test values type boolean");
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
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void jsonSourceMapperTest() throws InterruptedException {
        log.info("test JsonSourceMapper with test json object type");
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
        siddhiAppRuntime.shutdown();
    }


}
