package io.siddhi.extension.io.live.source.Stream.ZmqClient;

import io.siddhi.extension.io.live.source.Stream.ZmqClient.ActiveConsumerRecodHandling.ActiveMessageHandler;
import org.apache.tapestry5.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZThread;
import java.util.function.Consumer;
public class Subscriber implements ZThread.IAttachedRunnable
{
    private Consumer<String> consumer;
    public Subscriber(Consumer<String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
    {
        ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB);
        subscriber.connect("tcp://localhost:6000");
        subscriber.subscribe("networkTraffic".getBytes(ZMQ.CHARSET));

        ActiveMessageHandler<String,String> activeMessageHandler = new ActiveMessageHandler<>();
        activeMessageHandler.setConsumer(consumer);
//        activeMessageHandler.start();

        while (true) {
            String stringJsonMsg = subscriber.recvStr();
            JSONObject jsonObject = new JSONObject("{" + stringJsonMsg + "}");
            JSONObject newValue = ((JSONObject) (jsonObject.get("networkTraffic")));
            String value = newValue.toString();

            consumer.accept(value);
        }
    }
}
