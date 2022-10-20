package io.siddhi.extension.io.live.source;

public class StoppedThreadState extends ThreadState {
    public StoppedThreadState(StreamThread streamThread) {
        super(streamThread);
    }

    @Override
    protected void stop() {
        System.out.println("Thread has already stopped."); // TODO :  throw an exception
    }

    @Override
    protected void pause() {
        System.out.println("Thread has already stopped."); // TODO :  throw an exception
    }

    @Override
    protected void resume() {
        System.out.println("Thread has stopped."); // TODO :  throw an exception
    }
}
