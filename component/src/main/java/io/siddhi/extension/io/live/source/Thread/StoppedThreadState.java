package io.siddhi.extension.io.live.source.Thread;


public class StoppedThreadState extends ThreadState {
    public StoppedThreadState(AbstractThread streamThread) {
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
