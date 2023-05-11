package io.siddhi.extension.io.live.source.Stream;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.source.Stream.ZmqClient.Subscriber;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import lombok.Builder;
import org.zeromq.ZContext;
import org.zeromq.ZThread;

import java.util.function.Consumer;
import java.util.logging.Logger;

@Builder
public class StreamThread extends AbstractThread {
    private final static Logger LOGGER = Logger.getGlobal();
    private IStreamingEngine<String> IStreamingEngine;
    @Builder.Default private final Runtime JVMRuntime = Runtime.getRuntime();;
    private SourceEventListener sourceEventListener;
    Subscriber subscriber;

    private void unsubscribe(){
        IStreamingEngine.unsubscribe();
    }

    private void shutdown(){
        this.threadState.stop(); // since while shutting down , still thread may wait for message, so thread should be stopped to exit from while loop
        this.unsubscribe();
    }

    private void subscribe(){
        // this is simple temp fix. ideal is adding a state for handling unsubscribe when user wants
        JVMRuntime.addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdown(); // to handle process interruptions eg. ctrl+c
            }
        });

        IStreamingEngine.subscribe();

    }

    @Override
    public void run() {

//        this.subscribe();

        Consumer<String> sourceEventListenerSiddhi = (msg)->{
            sourceEventListener.onEvent(msg,null);
        };
        subscriber = new Subscriber(sourceEventListenerSiddhi);

        ZContext ctx = new ZContext();
        ZThread.fork(ctx, new Subscriber(sourceEventListenerSiddhi));

//        while(isThreadRunning){
//
//            if(isPaused) {
//                LOGGER.log(Level.INFO,"paused - stream thread");
//                doPause();
//            }
//            IStreamingEngine.consumeMessage(sourceEventListenerSiddhi);
//        }

        // clean exit if thread is stopped
//        this.unsubscribe();
    }
}
