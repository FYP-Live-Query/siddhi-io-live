package io.siddhi.extension.io.live.source.Thread;

import io.siddhi.extension.io.live.source.Stream.StreamThread;

public class PausedThreadState extends ThreadState {

    public PausedThreadState(StreamThread streamThread) {
        super(streamThread);
    }

    @Override
    public void stop() {
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
    public void pause() {
        // TODO :  throw an exception
        // Do nothing or throw exception saying thread is already paused
        System.out.println("Thread has already paused");
    }

    @Override
    public void resume() {
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
