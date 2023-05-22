package io.siddhi.extension.io.live.source.Stream.ZmqClient.ActiveConsumerRecodHandling;


import java.util.concurrent.*;
import java.util.function.Consumer;

public class ActiveMessageHandler<ValueType> {
    private final BlockingQueue<String> messages;
    private Consumer<ValueType> consumer;

    public ActiveMessageHandler() {
        this.messages = new LinkedBlockingQueue<>();
    }

    public void start(){
        if(consumer == null){
            throw new IllegalArgumentException("Consumer for message not set");
        }
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    String message = messages.take();
                    consumer.accept((ValueType) message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        );
            thread.start();
        }

    public void setConsumer(Consumer<ValueType> consumer) {
        this.consumer = consumer;
    }

    public void addMessage(String message){
        this.messages.add(message);
    }
}
