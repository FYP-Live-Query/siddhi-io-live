package io.siddhi.extension.io.live.source;

public class PausedThreadState extends ThreadState {

    public PausedThreadState(StreamThread streamThread) {
        super(streamThread);
    }

    @Override
    protected void stop() {
        // stop thread
        if(streamThread.isThreadRunning()){
            streamThread.setThreadRunning(false);
        }else{
            // TODO :  throw an exception
            System.out.println("Thread has already stopped.");
        }
        streamThread.setThreadState(new StoppedThreadState(streamThread));
    }

    @Override
    protected void pause() {
        // TODO :  throw an exception
        // Do nothing or throw exception saying thread is already paused
        System.out.println("Thread has already paused");
    }

    @Override
    protected void resume() {
        // resume Thread
        if(streamThread.isPaused()){
            System.out.println("Resuming Thread - thread state p");
            streamThread.setPaused(false);
            streamThread.doResume();

        }else{
            // TODO :  throw an exception
            System.out.println("Thread has already resumed.");
        }
        streamThread.setThreadState(new RunningThreadState(streamThread));
    }
}
