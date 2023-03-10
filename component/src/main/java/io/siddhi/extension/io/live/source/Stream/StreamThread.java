package io.siddhi.extension.io.live.source.Stream;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.utils.Monitor;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;

import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamThread extends AbstractThread {
    private final static Logger LOGGER = Logger.getGlobal();
    private final IStreamingEngine<String> IStreamingEngine;
    private final String topicOfStream;
    private final Runtime JVMRuntime;
    private final SourceEventListener sourceEventListener;

    public StreamThread(String topicOfStream,IStreamingEngine<String> iStreamingEngine, Monitor signalMonitor,
                        SourceEventListener sourceEventListener) {
        super(signalMonitor);
        this.topicOfStream = topicOfStream;
        this.sourceEventListener = sourceEventListener;
        this.IStreamingEngine = iStreamingEngine;
        this.JVMRuntime = Runtime.getRuntime();
    }

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

            IStreamingEngine.consumeMessage((str)->{
                sourceEventListener.onEvent(str,null);
            });
            
        }

        // clean exit if thread is stopped
        unsubscribe();
    }
}
