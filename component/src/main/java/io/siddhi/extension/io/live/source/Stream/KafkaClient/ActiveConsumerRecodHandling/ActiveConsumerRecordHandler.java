package io.siddhi.extension.io.live.source.Stream.KafkaClient.ActiveConsumerRecodHandling;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.tapestry5.json.JSONObject;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveConsumerRecordHandler<KeyType, ValueType> {
//    private final Logger logger = (Logger) LoggerFactory.getLogger(ActiveConsumerRecordHandler.class.getName());
    private final BlockingQueue<ConsumerRecords<KeyType, ValueType>> consumerRecordsList;
    private java.util.function.Consumer<ValueType> consumer;

    public ActiveConsumerRecordHandler() {
        this.consumerRecordsList = new LinkedBlockingQueue<>();
    }

    public void start(){
            if(consumer == null){
                throw new IllegalArgumentException("Consumer for message not set");
            }
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        ConsumerRecords<KeyType, ValueType> consumerRecords = consumerRecordsList.take();
                        for (ConsumerRecord<KeyType, ValueType> consumerRecord : consumerRecords) {
                            String stringJsonMsg = consumerRecord.value().toString();
                            JSONObject jsonObject = new JSONObject(stringJsonMsg);
                            JSONObject newValue = (JSONObject) ((JSONObject) jsonObject.get("payload")).get("after");

                            newValue.put("initial_data", "false"); // as required by the backend processing

                            JSONObject obj = new JSONObject();
                            obj.put("properties", newValue); // all user required data for siddhi processing inside properties section in JSON object
                            String strMsg = obj.toString();

                            consumer.accept((ValueType) strMsg); // The Java Consumer interface is a functional interface that represents an function that consumes a value without returning any value.
                        }

                    } catch (InterruptedException e) {
//                            logger.log(Level.ALL,e.getMessage());
                    }
                }
            }
        }
        );
            thread.start();
        }

    public void setConsumer(Consumer<ValueType> consumer) {
        this.consumer = consumer;
    }

    public void addConsumerRecords(ConsumerRecords<KeyType, ValueType> consumerRecords){
        this.consumerRecordsList.add(consumerRecords);
    }
}
