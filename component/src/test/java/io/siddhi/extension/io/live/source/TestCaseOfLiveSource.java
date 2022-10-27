package io.siddhi.extension.io.live.source;

import com.c8db.C8Cursor;
import com.c8db.C8DB;
import com.c8db.entity.BaseDocument;
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
                "@source(type='live',sql.query='FOR t IN network_traffic COLLECT browser = t.browser WITH COUNT INTO value RETURN {browser: browser,totalCount: value}', " +
                "host.name='api-varden-4f0f3c4f.paas.macrometa.io'," +
                "api.key = 'madu140_gmail.com." +
                "AccessPortal.2PL8EeyIAMn2sx7YHKWMM58tmJLES4NyIWq6Cnsj0BTMjygJyF3b14zb2sidcauXccccb8', " +
                " @map(type='json', fail.on.missing.attribute='false') )" +
                "define stream inputStream (id String,key String,revision String,properties String);";

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

}
