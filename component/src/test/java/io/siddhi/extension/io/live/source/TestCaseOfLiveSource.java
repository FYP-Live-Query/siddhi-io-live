package io.siddhi.extension.io.live.source;

// import com.c8db.C8Cursor;
// import com.c8db.C8DB;
// import com.c8db.entity.BaseDocument;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import SiddhiApp.Annotation.Attributes.JsonMapAttributes;
import SiddhiApp.Annotation.Common.KeyValue;
import SiddhiApp.Annotation.Info.QueryInfo;
import SiddhiApp.Annotation.Map.JsonMap;
import SiddhiApp.Annotation.Sink.LogSink;
import SiddhiApp.Annotation.Source.LiveSource;
import SiddhiApp.SiddhiApp;
import Compiler.SiddhiAppGenerator;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


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

        String inStreamDefinition0 = "@App:name('TestSiddhiApp0')" +
                "@source(type='live',sql.query='FOR t IN NetworkTrafficTable SORT t.traffic DESC LIMIT 5 RETURN t', " +
                "host.name='api-peamouth-0b57f3c7.paas.macrometa.io'," +
                "api.key = 'Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385', " +
                " @map(type = 'json',fail.on.missing.attribute = 'false',enclosing.element = '$.properties',@attributes(ip = 'ip')))" +
                "define stream inputStream (ip string);";

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
//
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
        String query0 = ("@sink(type = 'log')" +
                "define stream OutputStream (id String,key String,revision String,properties String);" +
                "@info(name = 'query0') "
                + "from inputStream "
                + "select * "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime0 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition0 + query0);

        siddhiAppRuntime0.addCallback("query0", new QueryCallback() {
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
    public void SQLtoSiddhiQLCompilerTest(){
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        String SQL = "SELECT ip@string FROM NetworkTrafficTable";
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
}
