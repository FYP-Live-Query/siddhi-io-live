package io.siddhi.extension.io.live.source.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.source.Stream.PulsarClient.IPulsarClientBehavior;
import io.siddhi.extension.io.live.utils.Monitor;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import org.apache.pulsar.client.api.*;
import org.apache.tapestry5.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class StreamThread extends AbstractThread {
    private final IPulsarClientBehavior pulsarClientBehavior;
    private final String topicOfStream;
    private final Runtime JVMRuntime;
    private final String subscriptionNameOfConsumer;
    private Consumer consumer;
    private final SourceEventListener sourceEventListener;

    public StreamThread(String topicOfStream,IPulsarClientBehavior pulsarClientBehavior,String subscriptionNameOfConsumer, Monitor signalMonitor,
                        SourceEventListener sourceEventListener) {
        super(signalMonitor);
        this.topicOfStream = topicOfStream;
        this.subscriptionNameOfConsumer = subscriptionNameOfConsumer;
        this.sourceEventListener = sourceEventListener;
        this.pulsarClientBehavior = pulsarClientBehavior;
        this.JVMRuntime = Runtime.getRuntime();
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
        JVMRuntime.addShutdownHook(new Thread(){ // this is simple temp fix. ideal is adding a state for handling unsubscribe when user wants
            @Override
            public void run() {
                unsubscribe();
            }
        });
        try {

            PulsarClient pulsarClient = pulsarClientBehavior.getPulsarClient();

            consumer = pulsarClient.newConsumer()
                    .topic(topicOfStream)
                    .subscriptionName(subscriptionNameOfConsumer)
                    .subscriptionType(SubscriptionType.Shared)
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
                JSONObject obj = new JSONObject();
                String stringJsonMsg = new String(msg.getData(), StandardCharsets.UTF_8);
                JSONObject jsonObject = new JSONObject(stringJsonMsg);
                jsonObject.put("initial_data", "false");
                obj.put("properties", jsonObject);
                String str = obj.toString();
                sourceEventListener.onEvent(str,null);

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
