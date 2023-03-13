package io.siddhi.extension.io.live.source.Stream;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.utils.Monitor;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import lombok.AccessLevel;
import lombok.Builder;

import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

@Builder
public class StreamThread extends AbstractThread {
    private final static Logger LOGGER = Logger.getGlobal();
    private IStreamingEngine<String> IStreamingEngine;
    private String topicOfStream;
    @Builder.Default private final Runtime JVMRuntime = Runtime.getRuntime();;
    private SourceEventListener sourceEventListener;

    private void unsubscribe(){
        IStreamingEngine.unsubscribe();
    }

    private void subscribe(){
        JVMRuntime.addShutdownHook(new Thread(){ // this is simple temp fix. ideal is adding a state for handling unsubscribe when user wants
            @Override
            public void run() {
                unsubscribe();
            }
        });
        IStreamingEngine.subscribe(topicOfStream);
    }

    @Override
    public void run() {
        subscribe();
        while(isThreadRunning){

            if(isPaused) {
                LOGGER.log(Level.INFO,"paused - stream thread");
                doPause();
            }

            Consumer<String> sourceEventListenerSiddhi = (msg)->{
                sourceEventListener.onEvent(msg,null);
            };

            IStreamingEngine.consumeMessage(sourceEventListenerSiddhi);
            
        }

        // clean exit if thread is stopped
        unsubscribe();
    }
}
