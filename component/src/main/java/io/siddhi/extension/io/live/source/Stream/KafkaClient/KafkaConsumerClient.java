package io.siddhi.extension.io.live.source.Stream.KafkaClient;

import io.siddhi.extension.io.live.source.Stream.IStreamingEngine;
import io.siddhi.extension.io.live.source.Stream.KafkaClient.ActiveConsumerRecodHandling.ActiveConsumerRecordHandler;
import lombok.Builder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Builder
public class KafkaConsumerClient<KeyType,ValueType> implements IStreamingEngine<ValueType> {

    private Consumer<KeyType, ValueType> kafkaConsumer;
    private String bootstrap_server_config;
    private Class key_deserializer_class_config;
    private Class value_deserializer_class_config;
    private String group_id_config;
    private String client_id_config;
    private String topic;
    private final AtomicBoolean waitingInterrupted = new AtomicBoolean(false);
    private ActiveConsumerRecordHandler<KeyType,ValueType> activeConsumerRecordHandler;

    private final Object lock = new Object();


    @Override
    public void consumeMessage(java.util.function.Consumer<ValueType> consumer) {
        activeConsumerRecordHandler.setConsumer(consumer);
        activeConsumerRecordHandler.start();
        while(!waitingInterrupted.get()) {
            synchronized (lock) {
                try {
                    ConsumerRecords<KeyType, ValueType> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));
                    System.out.println("polloing");
                    activeConsumerRecordHandler.addConsumerRecords(consumerRecords);
                    kafkaConsumer.commitAsync();
                } catch (WakeupException e) {
                    if (!waitingInterrupted.get()) {
                        throw e;
                    }
                }
            }
        }
    }

    private void initiateKafkaConsumer(){
        Properties consumerProps = new Properties();

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,this.bootstrap_server_config);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.key_deserializer_class_config);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.value_deserializer_class_config);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,this.group_id_config);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG,this.client_id_config);

        this.kafkaConsumer = new KafkaConsumer<>(consumerProps);
    }

    @Override
    public void subscribe() {
        // subscribe to topic
        synchronized (lock) {
            waitingInterrupted.set(false);
            if (this.kafkaConsumer == null) {
                this.initiateKafkaConsumer();
            }
            kafkaConsumer.subscribe(Collections.singleton(topic));
        }
    }

    @Override
    public void unsubscribe() {
        interruptWaiting(); // interrupts waiting for kafka message
        synchronized (lock) {
            if (kafkaConsumer == null || !waitingInterrupted.get()) {
                return;
            }
            kafkaConsumer.unsubscribe();
        }
    }

    private void interruptWaiting() {
        if(waitingInterrupted.get()){
            return; // kafka consumer already waked up to close the subscription
        }
        waitingInterrupted.set(true);
        kafkaConsumer.wakeup(); // interrupts if thread is waiting for message
    }
}
