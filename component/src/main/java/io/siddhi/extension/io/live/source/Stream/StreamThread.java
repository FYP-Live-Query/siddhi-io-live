package io.siddhi.extension.io.live.source.Stream;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.utils.Monitor;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.nio.charset.StandardCharsets;

public class StreamThread extends AbstractThread {
    private IPulsarClientBehavior pulsarClientBehavior;
    private String topicOfStream;
    private String subscriptionNameOfConsumer;
    private Consumer consumer;
    private final SourceEventListener sourceEventListener;

    public StreamThread(String topicOfStream,
                        IPulsarClientBehavior pulsarClientBehavior,
                        String subscriptionNameOfConsumer, Monitor signalMonitor,
                        SourceEventListener sourceEventListener) {
        super(signalMonitor);
        this.topicOfStream = topicOfStream;
        this.subscriptionNameOfConsumer = subscriptionNameOfConsumer;
        this.sourceEventListener = sourceEventListener;
        this.pulsarClientBehavior = pulsarClientBehavior;
    }

    private void unsubscribe(){
        try {
            consumer.unsubscribe();
            System.out.println("consumer unsubscribed to the stream");
        } catch (PulsarClientException ex) {
            ex.printStackTrace();
        }
    }

    private void subscribe(){
        try {

            PulsarClient pulsarClient = pulsarClientBehavior.getPulsarClient();

            consumer = pulsarClient.newConsumer()
                    .topic(topicOfStream)
                    .subscriptionName(subscriptionNameOfConsumer)
                    .subscribe();

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

        subscribe();

        while(isThreadRunning){
            Message msg = null;
            try {
                if(isPaused) {
                    System.out.println("paused - stream thread");
                    doPause();
                }
                msg = consumer.receive();
                sourceEventListener.onEvent(msg.getData(),null);
                String s = new String(msg.getData(), StandardCharsets.UTF_8);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }

            try {
                consumer.acknowledge(msg);
            } catch (PulsarClientException e) {
                consumer.negativeAcknowledge(msg);
                e.printStackTrace();
            }
        }

        // clean exit if thread is stopped
        unsubscribe();
    }
}
