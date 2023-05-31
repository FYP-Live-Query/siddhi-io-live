package io.siddhi.extension.io.live.source.Thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ThreadState {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractThread.class);

    protected AbstractThread thread;

    public ThreadState(AbstractThread thread) {
        this.thread = thread;
    }

    public void stop() {
        throw new UnsupportedOperationException("method not implemented in this state");
    }

    public  void pause(){
        throw new UnsupportedOperationException("method not implemented in this state");
    };

    public  void resume(){
        throw new UnsupportedOperationException("method not implemented in this state");
    };

}
