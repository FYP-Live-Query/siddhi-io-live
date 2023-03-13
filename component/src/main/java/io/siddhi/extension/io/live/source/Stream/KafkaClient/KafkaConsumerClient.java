package io.siddhi.extension.io.live.source.Stream.KafkaClient;

import io.siddhi.extension.io.live.source.Stream.IStreamingEngine;
import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.tapestry5.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Builder
public class KafkaConsumerClient<KeyType,ValueType> implements IStreamingEngine<ValueType> {

    private Consumer<KeyType, ValueType> kafkaConsumer;
    private String bootstrap_server_config;
    private Class key_deserializer_class_config;
    private Class value_deserializer_class_config;
    private String group_id_config;
    private String client_id_config;
    private String topic;


    @Override
    public void consumeMessage(java.util.function.Consumer<ValueType> consumer) {
        ConsumerRecords<KeyType, ValueType> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<KeyType, ValueType> consumerRecord : consumerRecords) {

            // TODO :  this should encapsulate (duplicate code)
            String stringJsonMsg = consumerRecord.value().toString();

            JSONObject jsonObject = new JSONObject(stringJsonMsg);
            JSONObject newValue = (JSONObject) ((JSONObject) jsonObject.get("payload")).get("after");

            newValue.put("initial_data", "false"); // as required by the backend processing

            JSONObject obj = new JSONObject();
            obj.put("properties", newValue); // all user required data for siddhi processing inside properties section in JSON object
            String strMsg = obj.toString();

            consumer.accept((ValueType) strMsg); // The Java Consumer interface is a functional interface that represents an function that consumes a value without returning any value.
        }
        kafkaConsumer.commitSync();
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
    public void subscribe(String topicOfStream) {
        // subscribe to topic
        if(this.kafkaConsumer == null){
            this.initiateKafkaConsumer();
        }
        kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public synchronized void unsubscribe() {
        kafkaConsumer.unsubscribe();
    }
}
