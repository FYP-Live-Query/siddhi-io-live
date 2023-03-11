package io.siddhi.extension.io.live.source.Stream.PulsarClient;

import lombok.Builder;
import lombok.NonNull;
import org.apache.pulsar.client.api.*;
import org.apache.tapestry5.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

@Builder
public class PulsarClientTLSAuth implements IPulsarClientBehavior {
    private final static Logger LOGGER = Logger.getGlobal();
    private String gdnAPIToken;
    private String serviceUrlOfPulsarServer;
    private Reader<byte[]> reader;

    @Override
    public PulsarClient getPulsarClient() throws PulsarClientException {

        return PulsarClient.builder()
                .serviceUrl(serviceUrlOfPulsarServer)
                .authentication(AuthenticationFactory.token(gdnAPIToken))
                .build();
    }

    @Override
    public void consumeMessage(@NonNull java.util.function.Consumer<String> consumer) {
        Message msg;
        try {
            msg = reader.readNext();
            // TODO :  this should encapsulate (duplicate code)
            JSONObject obj = new JSONObject();
            String stringJsonMsg = new String(msg.getData(), StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(stringJsonMsg);

            jsonObject.put("initial_data", "false"); // as needed by the backend processing
            obj.put("properties", jsonObject); // all user required data for siddhi processing inside properties section in JSON object
            String str = obj.toString();

            consumer.accept(str);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void subscribe(@NonNull String topicOfStream) {
        PulsarClient pulsarClient;
        // Create a reader on a topic and for a specific message (and onward)
        try {
            pulsarClient = this.getPulsarClient();
            reader = pulsarClient.newReader()
                    .topic(topicOfStream)
                    .startMessageId(MessageId.latest)
                    .create();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void unsubscribe() {
        try {
            reader.close();
            LOGGER.log(Level.INFO,"consumer unsubscribed to the stream");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
