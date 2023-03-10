package io.siddhi.extension.io.live.source.Stream.PulsarClient;

import org.apache.pulsar.client.api.*;
import org.apache.tapestry5.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PulsarClientTLSAuth implements IPulsarClientBehavior {
    private final static Logger LOGGER = Logger.getGlobal();
    private final String gdnAPIToken;
    private final String serviceUrlOfPulsarServer;
    private Reader<byte[]> reader;

    public PulsarClientTLSAuth(String gdnAPIToken, String serviceUrlOfPulsarServer) {
        this.gdnAPIToken = gdnAPIToken;
        this.serviceUrlOfPulsarServer = serviceUrlOfPulsarServer;
    }

    @Override
    public PulsarClient getPulsarClient() throws PulsarClientException {

        return PulsarClient.builder()
                .serviceUrl(serviceUrlOfPulsarServer)
                .authentication(AuthenticationFactory.token(gdnAPIToken))
                .build();
    }

    @Override
    public void consumeMessage(java.util.function.Consumer<String> consumer) {
        Message msg = null;
        try {
            msg = reader.readNext();

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
    public void subscribe(String topicOfStream) {
        PulsarClient pulsarClient = null;
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
