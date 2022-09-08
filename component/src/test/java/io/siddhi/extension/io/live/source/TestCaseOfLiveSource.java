package io.siddhi.extension.io.live.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.extension.map.xml.sourcemapper.XmlSourceMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
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
        logObj.info(" Creating test for publishing events without URL.");
//        URI baseURI = URI.create(String.format("http://%s:%d", "0.0.0.0", 8280));
        List<String> receivedEventNameList = new ArrayList<>(2);
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setExtension("xml-input-mapper", XmlSourceMapper.class);
        String inStreamDefinition = "@App:name('TestSiddhiApp')" +
                "@source(type='live'," +
                "sql.query='select count from network_traffic'," +
                "host.name='api-varden-4f0f3c4f.paas.macrometa.io'," +
                "api.key = 'madu140_gmail.com." +
                "AccessPortal.2PL8EeyIAMn2sx7YHKWMM58tmJLES4NyIWq6Cnsj0BTMjygJyF3b14zb2sidcauXccccb8'," +
                " @map(type='keyvalue'), @attributes(id = 'id', name = 'name'))," +
                "define stream inputStream (count int)";
        String query = ("@info(name = 'query') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;"
        );
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        siddhiAppRuntime.start();
        // publishing events
        List<String> expected = new ArrayList<>(1);
        expected.add("99");
//        expected.add("Mike");
//        String event1 = "<events>"
//                + "<event>"
//                + "<name>John</name>"
//                + "<age>100</age>"
//                + "<country>AUS</country>"
//                + "</event>"
//                + "</events>";
//        String event2 = "<events>"
//                + "<event>"
//                + "<name>Mike</name>"
//                + "<age>20</age>"
//                + "<country>USA</country>"
//                + "</event>"
//                + "</events>";
//        HttpTestUtil.httpPublishEventDefault(event1, baseURI);
//        HttpTestUtil.httpPublishEventDefault(event2, baseURI);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(receivedEventNameList.toString(), expected.toString());
        siddhiAppRuntime.shutdown();
    }
}
