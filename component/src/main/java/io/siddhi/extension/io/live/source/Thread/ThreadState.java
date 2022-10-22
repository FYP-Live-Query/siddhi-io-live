package io.siddhi.extension.io.live.source.Thread;

import io.siddhi.extension.io.live.source.Stream.StreamThread;

public abstract class ThreadState {

    protected StreamThread streamThread;

    public ThreadState(StreamThread streamThread) {
        this.streamThread = streamThread;
    }

    public abstract void stop();
    public abstract void pause();
    public abstract void resume();

}
