package io.siddhi.extension.io.live.source.Stream;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import lombok.Builder;

import java.util.function.Consumer;

@Builder
public class StreamThread extends AbstractThread {
    private IStreamingEngine<String> IStreamingEngine;
    @Builder.Default private final Runtime JVMRuntime = Runtime.getRuntime();;
    private SourceEventListener sourceEventListener;

    private void unsubscribe(){
        LOGGER.info("unsubscribe from stream");
        IStreamingEngine.unsubscribe();
    }

    private void shutdown(){
        LOGGER.info("shutting down stream thread");
        this.threadState.stop(); // since while shutting down , still thread may wait for message, so thread should be stopped to exit from while loop
        this.unsubscribe();
    }

    private void subscribe(){
        // this is simple temp fix. ideal is adding a state for handling unsubscribe when user wants
        JVMRuntime.addShutdownHook(new Thread(() -> {
            LOGGER.info("siddhi app is shutting down");
            shutdown(); // to handle process interruptions eg. ctrl+c
        }));

        IStreamingEngine.subscribe();

    }

    @Override
    public void run() {

        this.subscribe();

        Consumer<String> sourceEventListenerSiddhi = (msg)-> {
            sourceEventListener.onEvent(msg,null);
        };

        while(isThreadRunning){

            if(isPaused) {
                LOGGER.info("paused - stream thread");
                doPause();
            }
            IStreamingEngine.consumeMessage(sourceEventListenerSiddhi);
        }

        // clean exit if thread is stopped
        this.unsubscribe();
    }
}
