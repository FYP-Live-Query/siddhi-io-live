package io.siddhi.extension.io.live.source;

// import com.c8db.C8Cursor;
// import com.c8db.C8DB;
// import com.c8db.entity.BaseDocument;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiAppRuntimeBuilder;
import io.siddhi.core.util.parser.SiddhiAppParser;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.query.compiler.SiddhiCompiler;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testcase of LiveSource.
 */
public class TestCaseOfLiveSource implements Serializable {
    private static final Logger logObj = (Logger) LogManager.getLogger(TestCaseOfLiveSource.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private LinkedBlockingQueue<Event> linkedBlockingQueue = new LinkedBlockingQueue<>();
    private Thread thread = new Thread(new Runnable() {
        @SneakyThrows
        @Override
        public void run() {
            while(true) {
                Event e = linkedBlockingQueue.take();
                long time = System.currentTimeMillis() - Long.parseLong(e.getData()[3].toString());
                System.out.println(time);
//                Thread.sleep(100); // processing delyas
            }

        }
    });
    private int waitTime = 50;
    private int timeout = 30000;
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
//        List<String> receivedEventNameList = new ArrayList<>(2);
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition0 = "@app:name('SiddhiApp-dev-test')\n" +
                "@source(type = 'live',host.name = 'api-peamouth-0b57f3c7.paas.macrometa.io',api.key = 'Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385',sql.query = 'SELECT ip,browser,date, traffic, eventtimestamp FROM networktraffictable WHERE traffic > 9990000',@map(type = 'json',fail.on.missing.attribute = 'false',enclosing.element = '$.properties',@attributes(ip = 'ip',eventtimestamp = 'eventtimestamp',browser = 'browser',traffic = 'traffic',date = 'date')))\n" +
                "define stream networktraffictableInputStream(ip string,browser string,date string,traffic int,eventtimestamp long);\n" +
                "@sink(type = 'log')\n" +
                "define stream networktraffictableOutputStream(ip string,browser string,date string,traffic int,eventtimestamp long);\n" +
                "@info(name = 'SQL-SiddhiQL-dev-test')\n" +
                "from networktraffictableInputStream[traffic > 9990000 ]\n" +
                "select  ip  , browser  , date  , traffic  , eventtimestamp  \n" +
                "insert into networktraffictableOutputStream;";

        SiddhiAppRuntime siddhiAppRuntime0 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition0 /*+ query0*/);

        siddhiAppRuntime0.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime0.start();
        siddhiAppRuntime0.shutdown();
    }

    @Test
    public void SQLtoSiddhiQLCompilerWithC8DBTest(){
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String SQL = "SELECT ip@string,browser@string,date@string, " +
                "traffic@int, eventtimestamp@long FROM " +
                "NetworkTrafficTable WHERE traffic@int > 9990000";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("host.name","api-peamouth-0b57f3c7.paas.macrometa.io"))
                        .addSourceComposite(new KeyValue<>("api.key","Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385")),
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
            }
        });

        siddhiAppRuntime.start();

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void SQLtoSiddhiQLCompilerWithDebeziumMySQLTest() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String SQL = "SELECT  ip@string,  " +
                "browser@string, " +
                "sum(traffic@int) as sum_traffic, eventTimestamp@long, " +
                "date@string FROM table WHERE (browser@string = 'firefox' AND traffic@int > 20);";

        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("host.name","localhost:9092"))
                        .addSourceComposite(new KeyValue<>("api.key","")),
                new JsonMap()
                        .addMapComposite(new KeyValue<>("fail.on.missing.attribute","false"))
                        .addMapComposite(new KeyValue<>("enclosing.element","$.properties")),
                new JsonMapAttributes(),
                new LogSink(),
                new QueryInfo().setQueryName("SQL-SiddhiQL-dev-test")
        );

        String siddhiAppString = siddhiApp.getSiddhiAppStringRepresentation();
//        String siddhiAppString = "@app:name('SiddhiApp-dev-test')\n" +
//                "@source(type = 'live',host.name = 'localhost:9092',api.key = '',sql.query = \"SELECT  ip,  browser, sum(traffic) as sum_traffic, date FROM table WHERE (browser = 'firefox' AND traffic > 20);\",@map(type = 'json',fail.on.missing.attribute = 'false',enclosing.element = '$.properties',@attributes(ip = \"ip\",sum_traffic = 'sum_traffic',browser = 'browser',traffic = 'traffic',date = 'date')))\n" +
//                "define stream tableInputStream(ip string,browser string,traffic int,date string);\n" +
//                "@sink(type = 'log')\n" +
//                "define stream tableOutputStream(ip string,browser string,sum_traffic long,date string);\n" +
//                "@info(name = 'SQL-SiddhiQL-dev-test')\n" +
//                "from tableInputStream[( browser == 'firefox' AND traffic > 20 ) ]\n" +
//                "select  ip  , browser  ,sum( traffic )  as sum_traffic , date  \n" +
//                "insert into tableOutputStream;";
        System.out.println(siddhiAppString);

        persistenceStore.save("SiddhiApp-dev-test","table.name",siddhiApp.getTableName().getBytes());
        persistenceStore.save("SiddhiApp-dev-test","database.name","database".getBytes());
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);

        siddhiAppRuntime.addCallback("SQL-SiddhiQL-dev-test", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                // inEvents = [Event{timestamp=1681358165558, data=[194.198.49.98, firefox, 1062000, 1681358165556, 8/8/2100], isExpired=false}]
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
                linkedBlockingQueue.add(inEvents[0]);
            }
        });
        thread.start();

        siddhiAppRuntime.start();

        siddhiAppRuntime.shutdown();
        thread.join();
    }
}
