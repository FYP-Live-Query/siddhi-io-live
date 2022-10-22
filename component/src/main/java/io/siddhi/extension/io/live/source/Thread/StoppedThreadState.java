package io.siddhi.extension.io.live.source.Thread;

import io.siddhi.extension.io.live.source.Stream.StreamThread;

public class StoppedThreadState extends ThreadState {
    public StoppedThreadState(StreamThread streamThread) {
        super(streamThread);
    }

    @Override
    public void stop() {
        System.out.println("Thread has already stopped."); // TODO :  throw an exception
    }

    @Override
    public void pause() {
        System.out.println("Thread has already stopped."); // TODO :  throw an exception
    }

    @Override
    public void resume() {
        System.out.println("Thread has stopped."); // TODO :  throw an exception
    }
}
