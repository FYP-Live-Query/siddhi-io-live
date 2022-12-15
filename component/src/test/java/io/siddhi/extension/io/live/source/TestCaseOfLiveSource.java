package io.siddhi.extension.io.live.source;

import SiddhiApp.Annotation.Attributes.JsonMapAttributes;
import SiddhiApp.Annotation.Common.KeyValue;
import SiddhiApp.Annotation.Info.QueryInfo;
import SiddhiApp.Annotation.Map.JsonMap;
import SiddhiApp.Annotation.Sink.LogSink;
import SiddhiApp.Annotation.Source.LiveSource;
import SiddhiApp.SiddhiApp;
import Compiler.SiddhiAppGenerator;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testcase of LiveSource.
 */
public class TestCaseOfLiveSource {
    private static final Logger logObj = (Logger) LogManager.getLogger(TestCaseOfLiveSource.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private SiddhiManager siddhiManager;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        eventCount.set(0);
    }

    /**
     * Creating test.
     *
     * @throws Exception Interrupted exception
     */
    @Test
    public void liveSelect() throws Exception {
        String inStreamDefinition = "@app:name('TestSiddhiApp0')" +
                "@source(type='live',sql.query='FOR t IN NetworkTrafficTable COLLECT browser = t.browser WITH COUNT INTO value RETURN {browser: browser,totalCount: value}', " +
                "host.name='api-peamouth-0b57f3c7.paas.macrometa.io'," +
                "api.key = 'Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385', " +
                " @map(type='json', fail.on.missing.attribute='false') )\n" +
                "define stream inputStream (id String,key String,revision String,properties String);";

        String query = ("@sink(type = 'log')" +
                "define stream OutputStream (id String,key String,revision String,properties String);" +
                "@info(name = 'query0') "
                + "from inputStream "
                + "select * "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
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
    public void SQLtoSiddhiQLCompilerTest(){
        String SQL = "SELECT ip@string FROM NetworkTrafficTable";
        SiddhiApp siddhiApp = SiddhiAppGenerator.generateSiddhiApp(
                "SiddhiApp-dev-test",
                SQL,
                new LiveSource()
                        .addSourceComposite(new KeyValue<>("host.name","api-peamouth-0b57f3c7.paas.macrometa.io"))
                        .addSourceComposite(new KeyValue<>("api.key","Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385")),
                new JsonMap(),
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
