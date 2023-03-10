package io.siddhi.extension.io.live.source.Stream.PulsarClient;

import io.siddhi.extension.io.live.source.Stream.IStreamingEngine;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public interface IPulsarClientBehavior extends IStreamingEngine {

    PulsarClient getPulsarClient() throws PulsarClientException;
}
