package io.siddhi.extension.io.live.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import lombok.SneakyThrows;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import SiddhiAppComposites.Annotation.Attributes.JsonMapAttributes;
import SiddhiAppComposites.Annotation.Common.KeyValue;
import SiddhiAppComposites.Annotation.Info.QueryInfo;
import SiddhiAppComposites.Annotation.Map.JsonMap;
import SiddhiAppComposites.Annotation.Sink.LogSink;
import SiddhiAppComposites.Annotation.Source.LiveSource;
import SiddhiAppComposites.SiddhiAppGenerator;
import SiddhiAppComposites.SiddhiApp;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Testcase of LiveSource.
 */
public class TestCaseOfLiveSource implements Serializable {
    private static final Logger logObj = (Logger) LogManager.getLogger(TestCaseOfLiveSource.class);
    private final AtomicLong eventCount = new AtomicLong(0);
    private final AtomicLong sumtime = new AtomicLong(0);
    private final LinkedBlockingQueue<Event> linkedBlockingQueue = new LinkedBlockingQueue<>();
    private final Thread thread = new Thread(new Runnable() {
        @SneakyThrows
        @Override
        public void run() {
            while(true) {
                Event e = linkedBlockingQueue.take();
                long time = System.currentTimeMillis() - Long.parseLong(e.getData()[1].toString());
                System.out.println("Time:" + time + " avg:" + sumtime.get() / eventCount.incrementAndGet());
            }

        }
    });

    ExecutorService executorService = Executors.newFixedThreadPool(1000);
    @BeforeMethod
    public void init() {
        eventCount.set(0);
    }

    /**
     * Creating test.
     *
     * @throws Exception Interrupted exception
     */
    @Test
    public void liveSelect() throws Exception {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition0 = "@app:name('SiddhiApp-dev-test')\n" +
                "@source(type = 'live', table.name = 'networkTraffic', sql.query = 'SELECT ip,browser,date, traffic, eventtimestamp FROM networkTraffic WHERE traffic > 9990000',@map(type = 'json',fail.on.missing.attribute = 'false',enclosing.element = '$.properties',@attributes(ip = 'ip',eventtimestamp = 'eventtimestamp',browser = 'browser',traffic = 'traffic',date = 'date')))\n" +
                "define stream networktraffictableInputStream(ip string,browser string,date string,traffic int,eventtimestamp long);\n" +
                "@sink(type = 'log')\n" +
                "define stream networktraffictableOutputStream(ip string,browser string,date string,traffic int,eventtimestamp long);\n" +
                "@info(name = 'SQL-SiddhiQL-dev-test')\n" +
                "from networktraffictableInputStream[traffic > 9990000 ]\n" +
                "select  ip  , browser  , date  , traffic  , eventtimestamp  \n" +
                "insert into networktraffictableOutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition0 /*+ query0*/);

        siddhiAppRuntime.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }
            }
        });

        Thread siddhiAppThread = new Thread(siddhiAppRuntime::start);
        siddhiAppThread.start();
        siddhiAppThread.join();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void SQLtoSiddhiQLCompilerWithC8DBTest() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String SQL = "SELECT ip@string,browser@string,date@string, " +
                "traffic@int, eventtimestamp@long FROM " +
                "networkTraffic WHERE traffic@int > 9990000";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("table.name","networkTraffic")),
                new JsonMap()
                        .addMapComposite(new KeyValue<>("fail.on.missing.attribute","false"))
                        .addMapComposite(new KeyValue<>("enclosing.element","$.properties")),
                new JsonMapAttributes(),
                new LogSink(),
                new QueryInfo().setQueryName("SQL-SiddhiQL-dev-test")
        );

        String siddhiAppString =siddhiApp.getSiddhiAppStringRepresentation();
        System.out.println(siddhiAppString);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);

        siddhiAppRuntime.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }

                if (eventCount.get() > 50) {
                    siddhiAppRuntime.shutdown();
                }
            }
        });

        Thread siddhiAppThread = new Thread(siddhiAppRuntime::start);
        siddhiAppThread.start();
        siddhiAppThread.join();
    }

    @Test
    public void JoinQueriesTest() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String SQL = "SELECT order.orderId@string, item.itemType@string, item.unitPrice@float,order.totalRevenue@float, order.totalCost@float, order.totalProfit@float, order.eventTimestamp@long FROM item JOIN order ON item.itemType@string=order.itemType@string";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource(),
                new JsonMap()
                        .addMapComposite(new KeyValue<>("fail.on.missing.attribute","false"))
                        .addMapComposite(new KeyValue<>("enclosing.element","$.properties")),
                new JsonMapAttributes(),
                new LogSink()
                        .addSourceComposite(new KeyValue<>("priority","DEBUG")),
                new QueryInfo().setQueryName("SQL-SiddhiQL-dev-test")
        );

        String siddhiAppString = siddhiApp.getSiddhiAppStringRepresentation();
        
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);

        siddhiAppRuntime.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
               EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                    }

                    if (eventCount.get() > 50) {
                        siddhiAppRuntime.shutdown();
                    }
            }
        });
        
        Thread siddhiAppThread = new Thread(siddhiAppRuntime::start);
        siddhiAppThread.start();
        siddhiAppThread.join();
        Thread.sleep(50000);
    }
    @Test
    public void SQLtoSiddhiQLCompilerWithDebeziumMySQLTest() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String SQL ="SELECT  ip@string, eventTimestamp@long FROM networkTraffic";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource(),
                new JsonMap()
                        .addMapComposite(new KeyValue<>("fail.on.missing.attribute","false"))
                        .addMapComposite(new KeyValue<>("enclosing.element","$.properties")),
                new JsonMapAttributes(),
                new LogSink()
                        .addSourceComposite(new KeyValue<>("priority","DEBUG")),
                new QueryInfo().setQueryName("SQL-SiddhiQL-dev-test")
        );

//        String siddhiAppString = siddhiApp.getSiddhiAppStringRepresentation();
        String siddhiAppString = "@app:name(\"SiddhiApp-dev-test\")\n" +
                "@source(type = \"live\",sql.query = \"select order.orderId, item.itemType, item.unitPrice, order.totalRevenue, order.totalCost, order.totalProfit from item join order on item.itemType=order.itemType;\",table.name = \"item\",@map(type = \"json\",fail.on.missing.attribute = \"false\",enclosing.element = \"$.properties\",@attributes(totalRevenue = \"totalRevenue\",totalProfit = \"totalProfit\",itemType = \"itemType\",unitPrice = \"unitPrice\",totalCost = \"totalCost\",orderId = \"orderId\")))\n" +
                "define stream itemInputStream(itemType string,unitPrice float);\n" +
                "@source(type = \"live\",sql.query = \"select order.orderId, item.itemType, item.unitPrice, order.totalRevenue, order.totalCost, order.totalProfit from item join order on item.itemType=order.itemType;\",table.name = \"order\",@map(type = \"json\",fail.on.missing.attribute = \"false\",enclosing.element = \"$.properties\",@attributes(totalRevenue = \"totalRevenue\",totalProfit = \"totalProfit\",itemType = \"itemType\",unitPrice = \"unitPrice\",totalCost = \"totalCost\",orderId = \"orderId\")))\n" +
                "define stream orderInputStream(orderId string,totalRevenue float,totalCost float,totalProfit float,itemType string);\n" +
                "@sink(type = \"log\",priority = \"DEBUG\")\n" +
                "define stream itemOutputStream(orderId string,itemType string,unitPrice float,totalRevenue float,totalCost float,totalProfit float);\n" +
                "@info(name = \"SQL-SiddhiQL-dev-test\")\n" +
                "from itemInputStream#window.length(3) as item\n" +
                "join orderInputStream#window.length(3) as order\n" +
                "on item.itemType == order.itemType\n" +
                "select order.orderId, item.itemType, item.unitPrice, order.totalRevenue, order.totalCost, order.totalProfit\n" +
                "insert into itemOutputStream;\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);

        siddhiAppRuntime.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }

                if (eventCount.get() > 1000) {
                    siddhiAppRuntime.shutdown();
                }
            }
        });

        siddhiAppRuntime.start();
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test()
    public void SQLtoSiddhiQLCompilerWithDebeziumMySQLForMultipleSubscriptionsTest() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String SQL = "SELECT  ip@string, eventTimestamp@long FROM networkTraffic";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource(),
                new JsonMap()
                        .addMapComposite(new KeyValue<>("fail.on.missing.attribute","false"))
                        .addMapComposite(new KeyValue<>("enclosing.element","$.properties")),
                new JsonMapAttributes(),
                new LogSink()
                        .addSourceComposite(new KeyValue<>("priority","DEBUG")),
                new QueryInfo().setQueryName("SQL-SiddhiQL-dev-test")
        );

        String siddhiAppString = siddhiApp.getSiddhiAppStringRepresentation();

        List<SiddhiAppRuntime> siddhiAppRuntimes = new ArrayList<>();

        for (int i = 0; i < 150; i++) {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);
            siddhiAppRuntime.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                    }

                    if (eventCount.get() > 50) {
                        siddhiAppRuntime.shutdown();
                    }

                }
            });
            siddhiAppRuntimes.add(siddhiAppRuntime);
        }

        int i = 0;
        for (SiddhiAppRuntime siddhiAppRuntime :
                siddhiAppRuntimes) {
            Thread.sleep(50);
            System.out.println("starting siddhi app runtime : " +  ++i);
            executorService.execute(siddhiAppRuntime::start);
        }
        Thread.sleep(50000);
    }
}
