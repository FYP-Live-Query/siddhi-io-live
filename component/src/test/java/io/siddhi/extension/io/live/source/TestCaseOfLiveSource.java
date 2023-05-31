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

import org.apache.pulsar.shade.org.eclipse.util.thread.ExecutorThreadPool;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
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

                if (eventCount.get() > 50){
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

        String SQL = "SELECT table.a@string , stock.b@string FROM table JOIN stock ON table.id@string = stock.id@string";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("host.name","10.8.100.246:9092"))
                        .addSourceComposite(new KeyValue<>("api.key","")),
                new JsonMap()
                        .addMapComposite(new KeyValue<>("fail.on.missing.attribute","false"))
                        .addMapComposite(new KeyValue<>("enclosing.element","$.properties")),
                new JsonMapAttributes(),
                new LogSink().addSourceComposite(new KeyValue<>("priority","DEBUG")),
                new QueryInfo().setQueryName("default-name")
        );

//        String siddhiAppString = "@app:name(\"SiddhiApp-dev-test\")\n" +
//                "@source(type = \"live\",table.name = \"customers\", host.name=\"10.8.100.246:9092\", sql.query = \"SELECT customers.first_name , addresses.city FROM customers JOIN addresses ON customers.id = addresses.customer_id\",@map(type = \"json\",fail.on.missing.attribute= \"false\", enclosing.element = \"$.properties\", @attributes(first_name = \"first_name\",id = \"id\",city = \"city\",customer_id = \"customer_id\")))\n" +
//                "define stream customersInputStream(first_name string,id int);\n" +
//                "@source(type = \"live\",table.name = \"addresses\", host.name=\"10.8.100.246:9092\", sql.query = \"SELECT customers.first_name , addresses.city FROM customers JOIN addresses ON customers.id = addresses.customer_id\",@map(type = \"json\",fail.on.missing.attribute = \"false\", enclosing.element = \"$.properties\", @attributes(first_name = \"first_name\",id = \"id\",city = \"city\",customer_id = \"customer_id\")))\n" +
//                "define stream addressesInputStream(city string,customer_id int);\n" +
//                "@sink(type = \"log\")\n" +
//                "define stream customersOutputStream(first_name string,city string);\n" +
//                "@info(name = \"default-name\")\n" +
//                "from customersInputStream#window.length(3) as customers\n" +
//                "join addressesInputStream#window.length(3) as addresses\n" +
//                "on customers.id == addresses.customer_id\n" +
//                "select customers.first_name, addresses.city\n" +
//                "insert into customersOutputStream;\n";
        String siddhiAppString = siddhiApp.getSiddhiAppStringRepresentation();
        System.out.println(siddhiAppString);
        persistenceStore.save("SiddhiApp-dev-test","table.name",siddhiApp.getTableName().getBytes());
        persistenceStore.save("SiddhiApp-dev-test","database.name","inventory".getBytes());
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);

        siddhiAppRuntime.addCallback("default-name", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                // inEvents = [Event{timestamp=1681358165558, data=[194.198.49.98, firefox, 1062000, 1681358165556, 8/8/2100], isExpired=false}]
                linkedBlockingQueue.add(inEvents[0]);

            }
        });
//        thread.start();
        siddhiAppRuntime.start();

        Thread.sleep(500000);
//        thread.join();
        siddhiAppRuntime.shutdown();
    }
    @Test
    public void SQLtoSiddhiQLCompilerWithDebeziumMySQLTest() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String SQL = "SELECT  ip@string, eventTimestamp@long FROM networkTraffic";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("table.name","networkTraffic")),
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

                if (eventCount.get() > 50){
                    System.out.println("l");
                    siddhiAppRuntime.shutdown();
                }
            }
        });

        siddhiAppRuntime.start();
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
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("table.name","networkTraffic")),
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

        for (int i = 0; i < 1000; i++){
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);
            siddhiAppRuntime.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                    }

                    if (eventCount.get() > 50){
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
    }
}
