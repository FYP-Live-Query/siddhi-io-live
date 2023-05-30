package io.siddhi.extension.io.live.source.Thread;


public class RunningThreadState extends ThreadState {

    public RunningThreadState(AbstractThread streamThread) {
        super(streamThread);
    }

    @Override
    public void stop() {
        // stop thread
        if(thread.isThreadRunning()){
            thread.setThreadRunning(false);
        }else{
            // TODO :  throw an exception
            LOGGER.info("Thread has already stopped.");
        }
        thread.setThreadState(new StoppedThreadState(thread));
    }

    @Override
    public void pause() {
        if(thread.isPaused()){
            // TODO :  throw an exception
            LOGGER.info("Thread has already paused");
        }else{
            LOGGER.info("pasuing");
            thread.setPaused(true);
            thread.setThreadState(new PausedThreadState(thread));
        }
    }

    @Override
    public void resume() {
        // resumes Thread
        // TODO :  throw an exception
        LOGGER.info("Thread is already running.");
    }
}
