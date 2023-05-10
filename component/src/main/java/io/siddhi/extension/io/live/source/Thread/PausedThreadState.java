package io.siddhi.extension.io.live.source.Thread;

public class PausedThreadState extends ThreadState {

    public PausedThreadState(AbstractThread streamThread) {
        super(streamThread);
    }

    @Override
    public void stop() {
        // stop thread
        if(thread.isThreadRunning()){
            thread.setThreadRunning(false);
        }else{
            // TODO :  throw an exception
            System.out.println("Thread has already stopped.");

        }
        thread.setThreadState(new StoppedThreadState(thread));
    }

    @Override
    public void pause() {
        // TODO :  throw an exception
        // Do nothing or throw exception saying thread is already paused
        System.out.println("Thread has already paused");
    }

    @Override
    public void resume() {
        // resume Thread
        if(thread.isPaused()){
            System.out.println("Resuming Thread - thread state p");
            thread.setPaused(false);
            thread.doResume();

        }else{
            // TODO :  throw an exception
            System.out.println("Thread has already resumed.");
        }
        thread.setThreadState(new RunningThreadState(thread));
    }
}
