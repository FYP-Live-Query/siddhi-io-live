package io.siddhi.extension.io.live.source.Stream;

// This interface is to implement strategy design patter for
// differ between kafka,pulsar or any other new streaming engine
// according to user preference
public interface IStreamingEngine {
    void consumeMessage(); // this method will use by stream thread to consume message (strategy design pattern execute method)
}
