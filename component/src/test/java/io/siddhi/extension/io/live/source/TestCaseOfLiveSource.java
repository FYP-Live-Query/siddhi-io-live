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


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testcase of LiveSource.
 */
public class TestCaseOfLiveSource {
    private static final Logger logObj = (Logger) LogManager.getLogger(TestCaseOfLiveSource.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
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

//        String inStreamDefinition0 = "@App:name('TestSiddhiApp0')" +
//                "@source(type='live',sql.query='FOR t IN NetworkTrafficTable COLLECT browser = t.browser WITH COUNT INTO value RETURN {browser: browser,totalCount: value}', " +
//                "host.name='api-peamouth-0b57f3c7.paas.macrometa.io'," +
//                "api.key = 'Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385', " +
//                " @map(type='json', fail.on.missing.attribute='false') )" +
//                "define stream inputStream (id String,key String,revision String,properties String);";

//        String inStreamDefinition0 = "@app:name('SiddhiApp-dev-test')\n" +
//                "@source(type = 'live',host.name = 'api-peamouth-0b57f3c7.paas.macrometa.io',api.key = 'Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385',sql.query = 'SELECT ip FROM NetworkTrafficTable',@map(type = 'json',@attributes(ip = 'ip')))\n" +
//                "define stream NetworkTrafficTableInputStream(ip string);\n" +
//                "@sink(type = 'log')\n" +
//                "define stream NetworkTrafficTableOutputStream(ip string);\n" +
//                "@info(name = 'SQL-SiddhiQL-dev-test')\n" +
//                "from NetworkTrafficTableInputStream\n" +
//                "select  ip  \n" +
//                "insert into NetworkTrafficTableOutputStream;";
//        String query0 = ("@sink(type = 'log')" +
//                "define stream OutputStream (ip string);" +
//                "@info(name = 'query0') "
//                + "from inputStream "
//                + "select * "
//                + "insert into outputStream;");
//
//        SiddhiAppRuntime siddhiAppRuntime0 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition0 + query0);
//        siddhiAppRuntime0.addCallback("query0", new QueryCallback() {
//            @Override
//            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
//                for (Event event : inEvents) {
//                    eventCount.incrementAndGet();
//                }
//            }
//        });
//        siddhiAppRuntime0.start();
//        String query0 = ("@sink(type = 'log')" +
//                "define stream OutputStream (id String,key String,revision String,properties String);" +
//                "@info(name = 'query0') "
//                + "from inputStream "
//                + "select * "
//                + "insert into outputStream;");
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
        String SQL = "SELECT ip@string,browser@string,date@string, traffic@int, eventtimestamp@long FROM NetworkTrafficTable WHERE traffic@int > 9990000";
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
        String SQL = "SELECT ip@string,browser@string,date@string, stdDev(traffic@double) AS traffic_sum FROM table WHERE traffic@double > 90";
        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("host.name","localhost:9092"))
                        .addSourceComposite(new KeyValue<>("api.key","")),
                new JsonMap()
                        .addMapComposite(new KeyValue<>("fail.on.missing.attribute","true"))
                        .addMapComposite(new KeyValue<>("enclosing.element","$.properties")),
                new JsonMapAttributes(),
                new LogSink(),
                new QueryInfo().setQueryName("SQL-SiddhiQL-dev-test")
        );

        String siddhiAppString = siddhiApp.getSiddhiAppStringRepresentation();
        System.out.println(siddhiAppString);

//        //Siddhi Application
//        String siddhiApp = "" +
//                "define stream StockStream (symbol string, price float, volume long); " +
//                "" +
//                "@info(name = 'query1') " +
//                "from StockStream[volume < 150] " +
//                "select symbol, price " +
//                "insert into OutputStream;";
//





//         ConcurrentMap<String, SiddhiAppRuntime> siddhiAppRuntimeMap = new ConcurrentHashMap();
//
//        SiddhiContext siddhiContext = new SiddhiContext();
//        String updatedSiddhiApp = SiddhiCompiler.updateVariables(siddhiApp);
//
//        io.siddhi.query.api.SiddhiApp siddhiApp0 = SiddhiCompiler.parse(updatedSiddhiApp);
//
//        SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder  = SiddhiAppParser.parse(siddhiApp0, updatedSiddhiApp, siddhiContext);
//
//        siddhiAppRuntimeBuilder.setSiddhiAppRuntimeMap(siddhiAppRuntimeMap);
//        SiddhiAppRuntime siddhiAppRuntime = siddhiAppRuntimeBuilder.build();
//        siddhiAppRuntimeMap.put(siddhiAppRuntime.getName(), siddhiAppRuntime);

        persistenceStore.save("SiddhiApp-dev-test","table.name",siddhiApp.getTableName().getBytes());
        persistenceStore.save("SiddhiApp-dev-test","database.name","database".getBytes());
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

//        //Get InputHandler to push events into Siddhi
//        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
//
//        //Start processing
//        siddhiAppRuntime.start();
//
//
//        //Sending events to Siddhi
//        inputHandler.send(new Object[]{"IBM", 700f, 100L});
//        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
//        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
//        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
//        inputHandler.send(new Object[]{"WSO2", 45.6f, 50L});
//
////        QueryRuntime queryRuntime = new QueryRuntime()
//
////        siddhiAppRuntime.start();
//
////        siddhiAppRuntime.shutdown();

//                QueryRuntime queryRuntime = new QueryRuntime()

        siddhiAppRuntime.start();

        siddhiAppRuntime.shutdown();
    }
}
