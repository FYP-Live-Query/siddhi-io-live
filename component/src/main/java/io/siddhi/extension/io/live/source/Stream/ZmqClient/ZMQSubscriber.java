package io.siddhi.extension.io.live.source.Stream.ZmqClient;

import io.siddhi.extension.io.live.source.Stream.IStreamingEngine;
import io.siddhi.extension.io.live.utils.LiveSourceConstants;
import lombok.Builder;
import org.apache.tapestry5.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Builder
public class ZMQSubscriber implements  IStreamingEngine<String>
{
    private static Logger LOGGER = LoggerFactory.getLogger(ZMQSubscriber.class);
    private Consumer<String> consumer;
    private String kafkaServerHostIp;
    private int kafkaServerHostPort;
    private String kafkaTopic;
    private String zmqTopic;
    private int topicSubscriptionPort;
    private String ZMQBrokerServerHostIp;
    private int ZMQBrokerServerHostPort;
    private String[] columnNamesInterested;
    @Builder.Default private final AtomicBoolean interrupted = new AtomicBoolean(false);
    @Builder.Default private ZMQ.Socket subscriber = null;
    @Builder.Default private ZContext context = null;

    private void start()
    {

        while (!interrupted.get()) {
            String stringJsonMsg = subscriber.recvStr();
            JSONObject jsonObject = new JSONObject("{" + stringJsonMsg + "}");
            JSONObject newValue = ((JSONObject) (jsonObject.get(zmqTopic)));
            String value = newValue.toString();
            consumer.accept(value);
        }
        LOGGER.info(String.format("Unsubscribed to ZMQ local broker topic [%s]", zmqTopic));
        subscriber.disconnect(String.format("tcp://%s:%d", ZMQBrokerServerHostIp, topicSubscriptionPort));
        LOGGER.info(String.format("Disconnected from publisher at %s", String.format("tcp://%s:%d", ZMQBrokerServerHostIp, topicSubscriptionPort)));
    }

    @Override
    public void consumeMessage(Consumer<String> consumer) {
        this.consumer = consumer;
        this.start();
    }

    @Override
    public void subscribe() {
        this.context = new ZContext();
        LOGGER.info(String.format("Connecting to ZMQ local broker [tcp://%s:%d]", ZMQBrokerServerHostIp, ZMQBrokerServerHostPort));
        ZMQ.Socket socket = context.createSocket(SocketType.REQ);
        socket.connect(String.format("tcp://%s:%d", ZMQBrokerServerHostIp, ZMQBrokerServerHostPort));
        JSONObject request = new JSONObject().put(LiveSourceConstants.KAFKA_TOPIC, this.kafkaTopic)
                .put(LiveSourceConstants.KAFKA_SERVER_HOST, this.kafkaServerHostIp + ":" + this.kafkaServerHostPort)
                .put(LiveSourceConstants.COLUMN_NAME_FILTERING_ENABLED, columnNamesInterested != null)
                .put(LiveSourceConstants.COLUMN_NAMES, org.apache.tapestry5.json.JSONArray.from(Arrays.asList(this.columnNamesInterested)));

        LOGGER.info("Sending request " + request);
        socket.send(request.toString().getBytes(ZMQ.CHARSET), 0);

        byte[] reply = socket.recv(0);
        LOGGER.info("Received  " + new String(reply, ZMQ.CHARSET));

        JSONObject response = new JSONObject(new String(reply, ZMQ.CHARSET));

        this.subscriber = context.createSocket(SocketType.SUB);
        this.topicSubscriptionPort = Integer.parseInt(response.getString(LiveSourceConstants.ZMQ_TOPIC_PORT));
        this.zmqTopic = response.getString(LiveSourceConstants.ZMQ_TOPIC);

        subscriber.connect(String.format("tcp://%s:%d", ZMQBrokerServerHostIp, topicSubscriptionPort));
        subscriber.subscribe(zmqTopic.getBytes(ZMQ.CHARSET));
        LOGGER.info(String.format("Subscribed to ZMQ local broker topic [%s]", zmqTopic));
    }

    @Override
    public void unsubscribe() {
        if(interrupted.get()){
            return; // kafka consumer already waked up to close the subscription
        }
        interrupted.set(true);
    }
}
