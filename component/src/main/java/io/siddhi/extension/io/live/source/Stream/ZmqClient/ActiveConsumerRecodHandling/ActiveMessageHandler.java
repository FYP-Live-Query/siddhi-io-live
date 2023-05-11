package io.siddhi.extension.io.live.source.Stream.ZmqClient.ActiveConsumerRecodHandling;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.concurrent.*;
import java.util.function.Consumer;

public class ActiveMessageHandler<KeyType, ValueType> {
    private final BlockingQueue<String> messages;
    private Consumer<ValueType> consumer;
    private final ExecutorService executorService;

    public ActiveMessageHandler() {
        this.messages = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> runnablesBlockingQueue = new ArrayBlockingQueue<>(10); // for executor service
        this.executorService = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),Runtime.getRuntime().availableProcessors(),100, TimeUnit.MILLISECONDS, runnablesBlockingQueue);
    }

    public void start(){
            if(consumer == null){
                throw new IllegalArgumentException("Consumer for message not set");
            }
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    String message = messages.take();
                    executorService.execute(() -> {
                        consumer.accept((ValueType) message);
                    });
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
