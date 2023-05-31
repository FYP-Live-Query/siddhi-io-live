package io.siddhi.extension.io.live.source;


import io.siddhi.core.config.SiddhiContext;
import io.siddhi.extension.io.live.source.Stream.IStreamingEngine;
import io.siddhi.extension.io.live.source.Stream.StreamThread;
import io.siddhi.extension.io.live.source.Stream.ZmqClient.ZMQSubscriber;
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
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import io.siddhi.extension.io.live.utils.LiveExtensionConfig;
import io.siddhi.extension.io.live.utils.LiveSourceConstants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
                ),
                @Parameter(
                        name = "table.name",
                        description = "Database table name",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "database.name",
                        description = "Database name",
                        type = DataType.STRING
                )
        },
        examples = {
                @Example(
                        syntax = "@source(type = 'live', sql.query='Select * from table', " +
                                "\nhost.name='api-varden-example'," +
                        "\napi.key = 'apikey-xxxxxxxxx', database.name = 'databaseName', table.name = 'TableName', " +
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
    private String databaseServerHostName;
    private String apiKey;
    protected SourceEventListener sourceEventListener;
    private SiddhiContext siddhiContext;
    protected String[] requestedTransportPropertyNames;
    private StreamThread consumerThread;
    private String fullQualifiedTableName;
    private AbstractThread dbThread;
    private IStreamingEngine<String> streamingClient;
    private String serviceURLOfPulsarServer = "pulsar+ssl://%s:6651";
    private String ZMQBrokerServer = "tcp://%s";
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
            LiveExtensionConfig liveExtensionConfig = new LiveExtensionConfig.LiveExtensionConfigBuilder().build();
            siddhiAppName  =  siddhiAppContext.getName();
            this.siddhiContext = siddhiAppContext.getSiddhiContext();
            this.fullQualifiedTableName =
                    new String(siddhiContext.getPersistenceStore().load(siddhiAppName,"database.name"), StandardCharsets.UTF_8) + "." +
                            optionHolder.validateAndGetOption(LiveSourceConstants.TABLENAME).getValue();
            this.sourceEventListener = sourceEventListener;
            this.selectQuery = optionHolder.validateAndGetOption(LiveSourceConstants.SQLQUERY).getValue();
            this.databaseServerHostName = liveExtensionConfig.getProperty("databaseServerHost") == null ?
                    optionHolder.validateAndGetOption(LiveSourceConstants.HOSTNAME).getValue() : liveExtensionConfig.getProperty("databaseServerHost");
            this.apiKey = liveExtensionConfig.getProperty("apiKey") == null ?
                    optionHolder.validateAndGetOption(LiveSourceConstants.APIKEY).getValue() : liveExtensionConfig.getProperty("apiKey");
            this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();
            this.serviceURLOfPulsarServer = String.format(serviceURLOfPulsarServer, databaseServerHostName);
            this.ZMQBrokerServer = liveExtensionConfig.getProperty("ZMQBrokerServerHost") == null ?
                    String.format(ZMQBrokerServer,"localhost:5555") : String.format(ZMQBrokerServer,liveExtensionConfig.getProperty("ZMQBrokerServerHost"));

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

        String uuid = UUID.randomUUID().toString();

        streamingClient = ZMQSubscriber.builder()
                .topic("dbserver1." + this.fullQualifiedTableName)
                .databaseServer(databaseServerHostName)
                .ZMQBrokerServer(ZMQBrokerServer)
                .build();


//        streamingClient = KafkaConsumerClient.<String,String>builder()
//                            .bootstrap_server_config(hostName) // should we obtain hostname from Config management system?
//                            .key_deserializer_class_config(StringDeserializer.class)
//                            .value_deserializer_class_config(StringDeserializer.class)
//                            .group_id_config("siddhi-io-live-group-" + uuid) // new subscriber should be in new group for multicasts subscription
//                            .client_id_config("siddhi-io-live-group-client-" + uuid) // new subscriber should be in new group for multicasts subscriptio
//                            .topic("dbserver1." + this.fullQualifiedTableName) // should add table name
//                            .auto_offset_reset_config(AutoOffsetResetConfig.LATEST)
//                            .activeConsumerRecordHandler(new ActiveConsumerRecordHandler<>())
//                            .build();

//        dbThread = DBThread.builder()
//                            .sourceEventListener(sourceEventListener)
//                            .port(3306)
//                            .selectSQL(selectQuery)
//                            .hostName(hostName.substring(0,hostName.length() - 5))
//                            .username("root")
//                            .password("debezium")
//                            .dbName(new String(siddhiContext.getPersistenceStore().load(siddhiAppName,"database.name"), StandardCharsets.UTF_8))
//                            .build();
//        Thread threadDB = new Thread(dbThread, "Initial database thread");

        consumerThread = StreamThread.builder()
                            .sourceEventListener(sourceEventListener)
                            .IStreamingEngine(streamingClient)
                            .build();
        Thread threadCon = new Thread(consumerThread, "streaming thread");


        threadCon.start();
//        threadDB.start();
//        try {
//            threadCon.join();
////            threadDB.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * Called to pause event consumption.
     */
    @Override
    public void pause() {
        consumerThread.pause();
    }

    /**
     * Called to resume event consumption.
     */
    @Override
    public void resume() {
        consumerThread.resume();
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}.
     */
    @Override
    public void destroy() {
        consumerThread.stop();
    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {

    }
}
