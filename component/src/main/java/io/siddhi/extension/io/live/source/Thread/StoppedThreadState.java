package io.siddhi.extension.io.live.source.Thread;


public class StoppedThreadState extends ThreadState {
    public StoppedThreadState(AbstractThread streamThread) {
        super(streamThread);
    }

    @Override
    public void stop() {
        LOGGER.info("Thread has already stopped."); // TODO :  throw an exception
    }

    @Override
    public void pause() {
        LOGGER.info("Thread has already stopped."); // TODO :  throw an exception
    }

    @Override
    public void resume() {
        LOGGER.info("Thread has stopped."); // TODO :  throw an exception
    }
}
