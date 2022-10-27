package io.siddhi.extension.io.live.source.Stream;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarClientTLSAuth implements IPulsarClientBehavior{
    private String gdnAPIToken;
    private String serviceUrlOfPulsarServer;
    private PulsarClient pulsarClient;

    public PulsarClientTLSAuth(String gdnAPIToken, String serviceUrlOfPulsarServer) {
        this.gdnAPIToken = gdnAPIToken;
        this.serviceUrlOfPulsarServer = serviceUrlOfPulsarServer;
    }

    @Override
    public PulsarClient getPulsarClient() throws PulsarClientException {

        pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrlOfPulsarServer)
                .authentication(AuthenticationFactory.token(gdnAPIToken))
                .build();

        return pulsarClient;
    }
}
