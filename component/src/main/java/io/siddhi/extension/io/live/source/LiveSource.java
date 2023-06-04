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
import io.siddhi.extension.map.json.sourcemapper.JsonSourceMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
                        name = "database.name",
                        description = "Database name",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "table.name",
                        description = "Database table name",
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
    private static final Logger logger = LoggerFactory.getLogger(LiveSource.class);
    private String siddhiAppName;
    private String selectQuery;
    private String databaseServerHostIp;
    private int databaseServerHostPort;
    private int kafkaServerHostPort;
    private String kafkaServerHostIp;
    protected SourceEventListener sourceEventListener;
    private SiddhiContext siddhiContext;
    protected String[] requestedTransportPropertyNames;
    private StreamThread consumerThread;
    private String fullQualifiedTableName;
    private AbstractThread dbThread;
    private IStreamingEngine<String> streamingClient;
    private String databaseServerName;
    private String ZMQBrokerServerHostIp;
    private int ZMQBrokerServerHostPort;
    private String databaseName;
    private String tableName;

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
            this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();

            this.siddhiAppName  =  siddhiAppContext.getName();
            this.siddhiContext = siddhiAppContext.getSiddhiContext();
            this.sourceEventListener = sourceEventListener;

            this.tableName = optionHolder.validateAndGetOption(LiveSourceConstants.TABLE_NAME).getValue();
            this.selectQuery = optionHolder.validateAndGetOption(LiveSourceConstants.SQL_QUERY).getValue();

            this.databaseName = liveExtensionConfig.getProperty(LiveSourceConstants.DATABASE_NAME);
            this.databaseServerName = liveExtensionConfig.getProperty(LiveSourceConstants.DATABASE_SERVER_NAME);
            this.databaseServerHostIp = liveExtensionConfig.getProperty(LiveSourceConstants.DATABASE_SERVER_HOST_IP);
            this.databaseServerHostPort = Integer.parseInt(liveExtensionConfig.getProperty(LiveSourceConstants.DATABASE_SERVER_HOST_PORT));

            this.kafkaServerHostIp = liveExtensionConfig.getProperty(LiveSourceConstants.KAFKA_SERVER_HOST_IP);
            this.kafkaServerHostPort = Integer.parseInt(liveExtensionConfig.getProperty(LiveSourceConstants.KAFKA_SERVER_HOST_PORT));

            this.ZMQBrokerServerHostIp = liveExtensionConfig.getProperty(LiveSourceConstants.ZMQ_BROKER_SERVER_HOST_IP);
            this.ZMQBrokerServerHostPort = Integer.parseInt(liveExtensionConfig.getProperty(LiveSourceConstants.ZMQ_BROKER_SERVER_HOST_PORT));

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
        String[] columnNames;

        if ( sourceEventListener instanceof JsonSourceMapper){
            JsonSourceMapper jsonSourceMapper = (JsonSourceMapper) sourceEventListener;
            columnNames = jsonSourceMapper.getStreamDefinition().getAttributeNameArray();
            logger.info("columns this stream interested : " + Arrays.toString(columnNames));
        } else {
            columnNames = null;
            logger.warn("This extension does not use JsonSourceMapper. So unable to " +
                    "extract column names and Column name filtering is disabled.");
        }

        streamingClient = ZMQSubscriber.builder()
                .kafkaTopic(this.databaseServerName + "." + this.databaseName + "." + this.tableName)
                .kafkaServerHostIp(kafkaServerHostIp)
                .kafkaServerHostPort(kafkaServerHostPort)
                .ZMQBrokerServerHostIp(ZMQBrokerServerHostIp)
                .ZMQBrokerServerHostPort(ZMQBrokerServerHostPort)
                .columnNamesInterested(columnNames)
                .build();

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
        Thread consumerThreadActive = new Thread(consumerThread);
        consumerThreadActive.start();
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
        consumerThread.stop();
    }
}
