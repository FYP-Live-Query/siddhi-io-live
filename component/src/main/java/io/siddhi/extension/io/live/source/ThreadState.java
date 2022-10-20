package io.siddhi.extension.io.live.source;

public abstract class ThreadState {

    protected StreamThread streamThread;

    public ThreadState(StreamThread streamThread) {
        this.streamThread = streamThread;
    }

    protected abstract void stop();
    protected abstract void pause();
    protected abstract void resume();

}
