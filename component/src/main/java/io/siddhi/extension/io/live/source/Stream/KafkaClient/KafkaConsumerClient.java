package io.siddhi.extension.io.live.source.Stream.KafkaClient;

import io.siddhi.extension.io.live.source.Stream.IStreamingEngine;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Properties;

public class KafkaConsumerClient<KeyType,ValueType> implements IStreamingEngine<ValueType> {

    private Consumer<KeyType, ValueType> kafkaConsumer;

    public KafkaConsumerClient(Properties consumerProperties){
        this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    @Override
    public void consumeMessage(java.util.function.Consumer<ValueType> consumer) {
        ConsumerRecords<KeyType, ValueType> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<KeyType, ValueType> consumerRecord : consumerRecords) {
            consumer.accept(consumerRecord.value()); // The Java Consumer interface is a functional interface that represents an function that consumes a value without returning any value.
        }
        kafkaConsumer.commitSync();
    }

    @Override
    public void subscribe(String topicOfStream) {
        throw new UnsupportedOperationException("still on implementation");
    }

    @Override
    public void unsubscribe() {
        throw new UnsupportedOperationException("still on implementation");
    }
}
