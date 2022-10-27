package io.siddhi.extension.io.live.source.Stream;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarClientTLSAuth implements IPulsarClientBehavior{
    private final String gdnAPIToken;
    private final String serviceUrlOfPulsarServer;

    public PulsarClientTLSAuth(String gdnAPIToken, String serviceUrlOfPulsarServer) {
        this.gdnAPIToken = gdnAPIToken;
        this.serviceUrlOfPulsarServer = serviceUrlOfPulsarServer;
    }

    @Override
    public PulsarClient getPulsarClient() throws PulsarClientException {

        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrlOfPulsarServer)
                .authentication(AuthenticationFactory.token(gdnAPIToken))
                .build();

        return pulsarClient;
    }
}
