package io.siddhi.extension.io.live.source;


import com.c8db.C8Cursor;
import com.c8db.C8DB;
import com.c8db.entity.BaseDocument;

import com.google.gson.Gson;

import org.apache.pulsar.client.api.*;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.live.utils.LiveSourceConstants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


/**
 * This is a sample class-level comment, explaining what the extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Source configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type =  "Supported parameter types.
 *                                        eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                          according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type =   "Supported parameter types.
 *                                         eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                         according to the type."),
 * },
 * //If Source system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first  system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "the default value of the system parameter.",
 *                                      possibleParameter="the possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax = "sample query with Source annotation that explain how extension use in Siddhi."
 *                              description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */

@Extension(
        name = "live",
        namespace = "source",
        description = " ",
        parameters = {
                /*@Parameter(name = " ",
                        description = " " ,
                        dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }),
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }),*/
                @Parameter(
                        name = "sql.query",
                        description = "The SQL select query",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "host.name",
                        description = "The Hostname",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "api.key",
                        description = "The api Key",
                        type = DataType.STRING
                )
        },
        examples = {
                @Example(
                        syntax = "@source(type = 'live', sql.query='Select * from table', " +
                                "\nhost.name='api-varden-example'," +
                        "\napi.key = 'apikey-xxxxxxxxx', " +
                        "\n@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))" +
                        "\ndefine stream inputStream (id int, name string)",
                        description = "In this example, the Live source executes the select query. The" +
                        "events consumed by the source are sent to the inputStream"
                )
        }
)
// for more information refer https://siddhi.io/en/v5.0/docs/query-guide/#source
public class LiveSource extends Source {
    private static final Logger logger = LogManager.getLogger(LiveSource.class);
    private String siddhiAppName;
    private String selectQuery;
    private String hostName;
    private String apiKey;
    protected SourceEventListener sourceEventListener;
    protected String[] requestedTransportPropertyNames;
    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param sourceEventListener             The listener to pass the events for processing which are consumed
     *                                        by the source
     * @param optionHolder                    Contains static options of the source
     * @param requestedTransportPropertyNames Requested transport properties that should be passed to
     *                                        SourceEventListener
     * @param configReader                    System configuration reader for source
     * @param siddhiAppContext                Siddhi application context
     * @return StateFactory for the Function which contains logic for the updated state based on arrived events.
     */
    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
        String[] requestedTransportPropertyNames, ConfigReader configReader,
        SiddhiAppContext siddhiAppContext) {
            Map<String, String> deploymentConfigMap = new HashMap();
            deploymentConfigMap.putAll(configReader.getAllConfigs());
            siddhiAppName  =  siddhiAppContext.getName();
            this.sourceEventListener = sourceEventListener;
            this.selectQuery = optionHolder.validateAndGetOption(LiveSourceConstants.SQLQUERY).getValue();
            this.hostName = optionHolder.validateAndGetOption(LiveSourceConstants.HOSTNAME).getValue();
            this.apiKey = optionHolder.validateAndGetOption(LiveSourceConstants.APIKEY).getValue();
            this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();
        return null;
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[0];
    }

    /**
     * Give information to the deployment about the service exposed by the sink.
     *
     * @return ServiceDeploymentInfo  Service related information to the deployment
     */
    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * Initially Called to connect to the end point for start retrieving the messages asynchronously.
     *
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection. (can be used when events are receiving asynchronously)
     * @param state              current state of the source
     * @throws ConnectionUnavailableException if it cannot connect to the source backend immediately.
     */
    @Override
    public void connect(ConnectionCallback connectionCallback , State state) throws ConnectionUnavailableException {

        final C8DB c8db = new C8DB.Builder()
                .useSsl(true)
                .host(hostName , 443)
                .apiKey(apiKey)
                .user("root")
                .useSsl(true)
                .build();

        final C8Cursor<BaseDocument> cursor = c8db
                .db(null , "_system")
                .query(
                        selectQuery,
                        null,
                null,
                        BaseDocument.class
                );


        for (; cursor.hasNext();) {
            Gson gson = new Gson();
            String json = gson.toJson(cursor.next());
            sourceEventListener.onEvent(json , null);
        }

        PulsarClient client = null;

        try {
            client = PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650") // TODO : need c8db stream
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        Consumer consumer =
                null;
        try {
            consumer = client
            .newConsumer()
            .topic("my-topic")                          // TODO : topic name should be collection name
            .subscriptionName("my-subscription")        // TODO : should be an unique name
            .subscribe();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }


        Consumer finalConsumer = consumer;
        Runnable consumerThread =
                () -> {
                        int i = 0;
                        while (true) {

                            // Wait for a message
                            Message msg = null;
                            try {
                                assert finalConsumer != null;
                                msg = finalConsumer.receive();
                            } catch (PulsarClientException e) {
                                e.printStackTrace();
                            }

                            try {
                                // Do something with the message
                                assert msg != null;
                                sourceEventListener.onEvent(msg.getData() , null);

                                // Acknowledge the message so that it can be deleted by the message broker
                                finalConsumer.acknowledge(msg);
                            } catch (Exception e) {
                                // Message failed to process, redeliver later
                                finalConsumer.negativeAcknowledge(msg);
                            }
                        }
        };

        // TODO : need way to manage these threads
        Thread threadCon = new Thread(consumerThread);
        threadCon.start();

        try {
            threadCon.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Called to pause event consumption.
     */
    @Override
    public void pause() {

    }

    /**
     * Called to resume event consumption.
     */
    @Override
    public void resume() {

    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {

    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}.
     */
    @Override
    public void destroy() {

    }



}
