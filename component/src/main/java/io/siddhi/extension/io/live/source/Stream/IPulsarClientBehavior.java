package io.siddhi.extension.io.live.source.Stream;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public interface IPulsarClientBehavior {

    PulsarClient getPulsarClient() throws PulsarClientException;
}
