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

package org.wso2.extension.siddhi.map.json.sinkmapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.NoSuchAttributeException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonSinkMapperTestCase {
    private static final Logger log = Logger.getLogger(JSONOutputMapperWithSiddhiQueryAPITestCase.class);
    private final int waitTime = 2000;
    private final int timeout = 30000;
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
    }

    /*
    * Default json output mapping
    */
    @Test
    public void jsonSinkMapperTestCase1() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 1");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString , msg);
                        break;
                    case 2:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":57.678,\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    case 3:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":50.0,\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString , msg);
                        break;
                    case 4:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2#$%\",\"price\":50.0,\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString , msg);
                        break;
                    case 5:
                        jsonString = "[{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}," +
                                "{\"event\":{\"symbol\":\"IBM\",\"price\":32.6,\"volume\":160}}]";
                        AssertJUnit.assertEquals(jsonString , msg);
                        break;
                    default:
                        AssertJUnit.fail();
                }
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='json')) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};
        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});
        stockStream.send(new Object[]{"WSO2", 50f, 100L});
        stockStream.send(new Object[]{"WSO2#$%", 50f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});
        SiddhiTestHelper.waitForEvents(waitTime, 5, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals(5, wso2Count.get());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void jsonSinkMapperTestCase2() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 2");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":\"undefined\"}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    case 2:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":\"undefined\",\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    case 3:
                        jsonString = "{\"event\":{\"symbol\":\"undefined\",\"price\":55.6,\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    default:
                        AssertJUnit.fail();
                }
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='json')) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, null});
        stockStream.send(new Object[]{"WSO2", null, 100L});
        stockStream.send(new Object[]{null, 55.6f, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 3, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals(3, wso2Count.get());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void jsonSinkMapperTestCase3() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 3");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        jsonString = "{\"portfolio\":" +
                                "{\"company\":{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    case 2:
                        jsonString = "{\"portfolio\":" +
                                "{\"company\":{\"event\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":200}}}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    case 3:
                        jsonString = "{\"portfolio\":" +
                                "{\"company\":{\"event\":{\"symbol\":\"WSO2\",\"price\":57.6,\"volume\":300}}}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    default:
                        AssertJUnit.fail();
                }
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='json', " +
                "enclosing.element=\"$.portfolio.company\")) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 56.6f, 200L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 300L});
        SiddhiTestHelper.waitForEvents(waitTime, 3, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals(3, wso2Count.get());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void jsonSinkMapperTestCase4() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 3");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    case 2:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":101}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    default:
                        AssertJUnit.fail();
                }
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (ibmCount.incrementAndGet()) {
                    case 1:
                        jsonString = "{\"event\":{\"symbol\":\"IBM\",\"price\":75.6,\"volume\":200}}";
                        AssertJUnit.assertEquals(jsonString, msg);
                        break;
                    default:
                        AssertJUnit.fail();
                }
            }
            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', validate.json='true')) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 56.6f, 101L});
        stockStream.send(new Object[]{"IBM", 75.6f, 200L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals(2, wso2Count.get());
        AssertJUnit.assertEquals(1, ibmCount.get());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    /*
    * Custom json output mapping
    */
    @Test
    public void jsonSinkMapperTestCase5() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 5");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" +
                "@App:name('TestSiddhiApp') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', validate.json='true', " +
                "enclosing.element=\"$.portfolio.company\", " +
                "@payload(\"\"\"{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"{{symbol}}\",\n" +
                "      \"Price\":{{price}}\n" +
                "   }\n" +
                "}\"\"\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom json
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"WSO2\",\n" +
                "      \"Price\":55.6\n" +
                "   }\n" +
                "}}}", onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"IBM\",\n" +
                "      \"Price\":75.6\n" +
                "   }\n" +
                "}}}", onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"WSO2\",\n" +
                "      \"Price\":57.6\n" +
                "   }\n" +
                "}}}", onMessageList.get(2).toString());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void jsonSinkMapperTestCase6() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 6");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" +
                "@App:name('TestSiddhiApp') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', " +
                "validate.json='true', " +
                "@payload(\"{'StockData':{'Symbol':{{symbol}}},'Price':{{{price}}\")))" +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 0, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 0, ibmCount.get());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void jsonSinkMapperTestCase7() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 7");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" +
                "@App:name('TestSiddhiApp') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', " +
                "validate.json='false', " +
                "@payload(\"{'StockData':{'Symbol':{{symbol}}},'Price':{{{price}}\")))" +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom json
        AssertJUnit.assertEquals("Mapping incorrect!", "{'StockData:{'Symbol:WSO2},Price:{55.6",
                onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{'StockData:{'Symbol:IBM},Price:{75.6",
                onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{'StockData:{'Symbol:WSO2},Price:{57.6",
                onMessageList.get(2).toString());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void jsonSinkMapperTestCase8() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 8");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" +
                "@App:name('TestSiddhiApp') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', validate.json='true', " +
                "enclosing.element=\"$.portfolio.company\", " +
                "@payload(\"\"\"{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"{{symbol}}\",\n" +
                "      \"Price\":{{non-exist}}\n" +
                "   }\n" +
                "}\"\"\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            siddhiManager.createSiddhiAppRuntime(streams + query);
        } catch (Exception e) {
            AssertJUnit.assertEquals(NoSuchAttributeException.class, e.getClass());
        }
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void jsonSinkMapperTestCase9() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 9");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" +
                "@App:name('TestSiddhiApp') " +
                "define stream FooStream (symbol string, price float, volume long, country string); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', validate.json='true', " +
                "enclosing.element=\"$.portfolio.company\", " +
                "@payload(\"\"\"{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"{{symbol}}\",\n" +
                "      \"Price\":{{price}}\n" +
                "   }\n" +
                "}\"\"\"))) " +
                "define stream BarStream (symbol string, price float, volume long, country string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L, "SL"});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L, "USA"});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L, "SL"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom json
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"WSO2\",\n" +
                "      \"Price\":55.6\n" +
                "   }\n" +
                "}}}", onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"IBM\",\n" +
                "      \"Price\":75.6\n" +
                "   }\n" +
                "}}}", onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"WSO2\",\n" +
                "      \"Price\":57.6\n" +
                "   }\n" +
                "}}}", onMessageList.get(2).toString());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void jsonSinkMapperTestCase10() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 10");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }
            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" +
                "@App:name('TestSiddhiApp') " +
                "define stream FooStream (symbol string, price float, volume long, company string); " +
                "@sink(type='inMemory', topic='{{company}}', @map(type='json', validate.json='true', " +
                "enclosing.element=\"$.portfolio.company\", " +
                "@payload(\"\"\"{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"{{symbol}}\",\n" +
                "      \"Price\":{{price}},\n" +
                "      \"Volume\":{{volume}}\n" +
                "   }\n" +
                "}\"\"\"))) " +
                "define stream BarStream (symbol string, price float, volume long, company string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, null, "WSO2"});
        stockStream.send(new Object[]{"IBM", null, 500L, "IBM"});
        stockStream.send(new Object[]{null, 57.6f, 200L, "WSO2"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom json
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"WSO2\",\n" +
                "      \"Price\":55.6,\n" +
                "      \"Volume\":undefined\n" +
                "   }\n" +
                "}}}", onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"IBM\",\n" +
                "      \"Price\":undefined,\n" +
                "      \"Volume\":500\n" +
                "   }\n" +
                "}}}", onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Mapping incorrect!", "{\"portfolio\":{\"company\":{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"undefined\",\n" +
                "      \"Price\":57.6,\n" +
                "      \"Volume\":200\n" +
                "   }\n" +
                "}}}", onMessageList.get(2).toString());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void jsonSinkMapperTestCase11() throws InterruptedException {
        log.info("JsonSinkMapperTestCase with test custom multiple Event");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='WSO2', @map(type='json',"
                + " enclosing.element=\"$.portfolio.company\", " + "@payload(\"\"\"{\n" + "   \"Stock Data\":{\n"
                + "       \"Symbol\":\"{{symbol}}\",\n" + "       \"Price\":{{price}},\n"
                + "       \"Volume\":{{volume}}\n" + "   }\n" + "}\"\"\"))) "
                + "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = { "WSO2", 55.6f, 100L };
        Object[] ibmData = { "IBM", 32.6f, 160L };
        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[] { "WSO2", 55.6f, 100L });
        stockStream.send(new Object[] { "WSO2", 57.678f, 100L });
        stockStream.send(new Object[] { "WSO2", 50f, 100L });
        stockStream.send(new Object[] { "WSO2#$%", 50f, 100L });
        stockStream.send(new Event[] { wso2Event, ibmEvent });
        SiddhiTestHelper.waitForEvents(waitTime, 5, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals(5, wso2Count.get());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\"portfolio\":{\"company\":{\n" + "   \"Stock Data\":{\n" + "       \"Symbol\":\"WSO2\",\n"
                        + "       \"Price\":55.6,\n" + "       \"Volume\":100\n" + "   }\n" + "}}}",
                onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\"portfolio\":{\"company\":{\n" + "   \"Stock Data\":{\n" + "       \"Symbol\":\"WSO2\",\n"
                        + "       \"Price\":57.678,\n" + "       \"Volume\":100\n" + "   }\n" + "}}}",
                onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\"portfolio\":{\"company\":{\n" + "   \"Stock Data\":{\n" + "       \"Symbol\":\"WSO2\",\n"
                        + "       \"Price\":50,\n" + "       \"Volume\":100\n" + "   }\n" + "}}}",
                onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\"portfolio\":{\"company\":{\n" + "   \"Stock Data\":{\n" + "       \"Symbol\":\"WSO2#$%\",\n"
                        + "       \"Price\":50,\n" + "       \"Volume\":100\n" + "   }\n" + "}}}",
                onMessageList.get(3).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\"portfolio\":{\"company\":[{\n" + "   \"Stock Data\":{\n" + "       \"Symbol\":\"WSO2\",\n "
                        + "      \"Price\":55.6,\n" + "       \"Volume\":100\n " + "  }\n" + "},\n{\n"
                        + "   \"Stock Data\":{\n" + "       \"Symbol\":\"IBM\",\n " + "      \"Price\":32.6,\n"
                        + "       \"Volume\":160\n" + "   }\n" + "}]" + "}}", onMessageList.get(4).toString());
        siddhiAppRuntime.shutdown();

        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void jsonSinkMapperTestCase12() throws InterruptedException {
        log.info("JsonSinkMapperTestCase with test custom multiple event with out enclose element");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='WSO2', @map(type='json',"
                + "@payload(\"\"\"{\n" + "   \"Stock Data\":{\n"
                + "       \"Symbol\":\"{{symbol}}\",\n" + "       \"Price\":{{price}},\n"
                + "       \"Volume\":{{volume}}\n" + "   }\n" + "}\"\"\"))) "
                + "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = { "WSO2", 55.6f, 100L };
        Object[] ibmData = { "IBM", 32.6f, 160L };
        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[] { "WSO2", 55.6f, 100L });
        stockStream.send(new Object[] { "WSO2", 57.678f, 100L });
        stockStream.send(new Object[] { "WSO2", 50f, 100L });
        stockStream.send(new Object[] { "WSO2#$%", 50f, 100L });
        stockStream.send(new Event[] { wso2Event, ibmEvent });
        SiddhiTestHelper.waitForEvents(waitTime, 5, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals(5, wso2Count.get());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\n"       +
                        "   \"Stock Data\":{\n" +
                        "       \"Symbol\":\"WSO2\",\n" +
                        "       \"Price\":55.6,\n" +
                        "       \"Volume\":100\n" +
                        "   }\n" +
                        "}",
                onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\n"       +
                        "   \"Stock Data\":{\n" +
                        "       \"Symbol\":\"WSO2\",\n" +
                        "       \"Price\":57.678,\n" +
                        "       \"Volume\":100\n" +
                        "   }\n" +
                        "}",
                onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\n"       +
                        "   \"Stock Data\":{\n" +
                        "       \"Symbol\":\"WSO2\",\n" +
                        "       \"Price\":50,\n" +
                        "       \"Volume\":100\n" +
                        "   }\n" +
                        "}"
                , onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "{\n"       +
                        "   \"Stock Data\":{\n" +
                        "       \"Symbol\":\"WSO2#$%\",\n" +
                        "       \"Price\":50,\n" +
                        "       \"Volume\":100\n" +
                        "   }\n" +
                        "}", onMessageList.get(3).toString());
        AssertJUnit.assertEquals("Mapping incorrect!",
                "[{\n" + "   \"Stock Data\":{\n" + "       \"Symbol\":\"WSO2\",\n "
                        + "      \"Price\":55.6,\n" + "       \"Volume\":100\n " + "  }\n" + "},\n{\n"
                        + "   \"Stock Data\":{\n" + "       \"Symbol\":\"IBM\",\n " + "      \"Price\":32.6,\n"
                        + "       \"Volume\":160\n" + "   }\n" + "}]", onMessageList.get(4).toString());
        siddhiAppRuntime.shutdown();

        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void jsonSinkMapperTestCase13() throws InterruptedException {
        log.info("JsonSinkMapperTestCase with test type is boolean");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (wso2Count.incrementAndGet()) {
                case 1:
                    jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}";
                    AssertJUnit.assertEquals(jsonString, msg);
                    break;
                case 2:
                    jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":101}}";
                    AssertJUnit.assertEquals(jsonString, msg);
                    break;
                default:
                    AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (ibmCount.incrementAndGet()) {
                case 1:
                    jsonString = "{\"event\":{\"symbol\":\"IBM\",\"price\":75.6,\"volume\":200}}";
                    AssertJUnit.assertEquals(jsonString, msg);
                    break;
                default:
                    AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);
        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='json', validate.json='true')) "
                + "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[] { "WSO2", 55.6f, 100L });
        stockStream.send(new Object[] { "WSO2", 56.6f, 101L });
        stockStream.send(new Object[] { "IBM", 75.6f, 200L });
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals(2, wso2Count.get());
        AssertJUnit.assertEquals(1, ibmCount.get());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void jsonSinkMapperTestCase14() throws InterruptedException {
        log.info("JsonSinkMapperTestCase with test default MultipleEvent");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (wso2Count.incrementAndGet()) {
                case 1:
                    jsonString = "{\"portfolio\":{\"company\":{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,"
                            + "\"volume\":100}}}}";
                    AssertJUnit.assertEquals(jsonString , msg);
                    break;
                case 2:
                    jsonString = "{\"portfolio\":{\"company\":{\"event\":{\"symbol\":\"WSO2\",\"price\":57.678,"
                            + "\"volume\":100}}}}";
                    AssertJUnit.assertEquals(jsonString, msg);
                    break;
                case 3:
                    jsonString = "{\"portfolio\":{\"company\":{\"event\":{\"symbol\":\"WSO2\",\"price\":50.0,"
                            + "\"volume\":100}}}}";
                    AssertJUnit.assertEquals(jsonString , msg);
                    break;
                case 4:
                    jsonString = "{\"portfolio\":{\"company\":{\"event\":{\"symbol\":\"WSO2#$%\",\"price\":50.0,"
                            + "\"volume\":100}}}}";
                    AssertJUnit.assertEquals(jsonString , msg);
                    break;
                case 5:
                    jsonString = "{\"portfolio\":{\"company\":[{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,"
                            + "\"volume\":100}}," +
                            "{\"event\":{\"symbol\":\"IBM\",\"price\":32.6,\"volume\":160}}]}}";
                    AssertJUnit.assertEquals(jsonString , msg);
                    break;
                default:
                    AssertJUnit.fail();
                }
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='json'," +
                " enclosing.element=\"$.portfolio.company\")) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};
        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});
        stockStream.send(new Object[]{"WSO2", 50f, 100L});
        stockStream.send(new Object[]{"WSO2#$%", 50f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});
        SiddhiTestHelper.waitForEvents(waitTime, 5, wso2Count, timeout);
        //assert event count
        AssertJUnit.assertEquals(5, wso2Count.get());
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }
}
