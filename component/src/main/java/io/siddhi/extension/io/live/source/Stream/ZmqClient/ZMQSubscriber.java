package io.siddhi.extension.io.live.source.Stream.ZmqClient;

import io.siddhi.extension.io.live.source.Stream.IStreamingEngine;
import lombok.Builder;
import org.apache.tapestry5.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

@Builder
public class ZMQSubscriber implements  IStreamingEngine<String>
{
    private static Logger LOGGER = Logger.getLogger(ZMQSubscriber.class.toString());
    private Consumer<String> consumer;
    private String kafkaServerHostIp;
    private int kafkaServerHostPort;
    private String topic;
    private int topicSubscriptionPort;
    private String ZMQBrokerServerHostIp;
    private int ZMQBrokerServerHostPort;
    @Builder.Default private final AtomicBoolean interrupted = new AtomicBoolean(false);
    @Builder.Default private ZMQ.Socket subscriber = null;
    @Builder.Default private ZContext context = null;

    private void start()
    {

        while (!interrupted.get()) {
            String stringJsonMsg = subscriber.recvStr();
            JSONObject jsonObject = new JSONObject("{" + stringJsonMsg + "}");
            JSONObject newValue = ((JSONObject) (jsonObject.get(topic)));
            String value = newValue.toString();
            System.out.println("Value: " + value);
            consumer.accept(value);
        }

        LOGGER.log(Level.INFO, String.format("Unsubscribed to ZMQ local broker topic [%s]", topic));
        subscriber.disconnect(String.format("tcp://%s:%d", ZMQBrokerServerHostIp, topicSubscriptionPort));
        LOGGER.log(Level.INFO, String.format("Disconnected from publisher at %s", String.format("tcp://%s:%d", ZMQBrokerServerHostIp, topicSubscriptionPort)));
    }

    @Override
    public void consumeMessage(Consumer<String> consumer) {
        this.consumer = consumer;
        this.start();
    }

    @Override
    public void subscribe() {
        this.context = new ZContext();
        LOGGER.log(Level.INFO, String.format("Connecting to ZMQ local broker [tcp://%s:%d]", ZMQBrokerServerHostIp, ZMQBrokerServerHostPort));

        ZMQ.Socket socket = context.createSocket(SocketType.REQ);
        socket.connect(String.format("tcp://%s:%d", ZMQBrokerServerHostIp, ZMQBrokerServerHostPort));
        JSONObject request = new JSONObject().put("topic", this.topic)
                .put("kafka.server.host", this.kafkaServerHostIp + ":" + this.kafkaServerHostPort);

        LOGGER.log(Level.INFO, "Sending request " + request);
        socket.send(request.toString().getBytes(ZMQ.CHARSET), 0);

        byte[] reply = socket.recv(0);
        LOGGER.log(Level.INFO, "Received  " + new String(reply, ZMQ.CHARSET));

        JSONObject response = new JSONObject(new String(reply, ZMQ.CHARSET));

        this.subscriber = context.createSocket(SocketType.SUB);
        this.topicSubscriptionPort = Integer.parseInt(response.getString("port"));

        subscriber.connect(String.format("tcp://%s:%d", ZMQBrokerServerHostIp, topicSubscriptionPort));
        subscriber.subscribe(topic.getBytes(ZMQ.CHARSET));
        LOGGER.log(Level.INFO, String.format("Subscribed to ZMQ local broker topic [%s]", topic));
    }

    @Override
    public void unsubscribe() {
        if(interrupted.get()){
            return; // kafka consumer already waked up to close the subscription
        }
        interrupted.set(true);
    }
}
