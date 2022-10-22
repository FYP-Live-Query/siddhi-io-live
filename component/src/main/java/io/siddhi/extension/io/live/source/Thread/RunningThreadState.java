package io.siddhi.extension.io.live.source.Thread;

import io.siddhi.extension.io.live.source.Stream.StreamThread;

public class RunningThreadState extends ThreadState {

    public RunningThreadState(StreamThread streamThread) {
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
        if(streamThread.isPaused()){
            // TODO :  throw an exception
            System.out.println("Thread is already paused");
        }else{
            System.out.println("pasuing");
            streamThread.setPaused(true);
            streamThread.setThreadState(new PausedThreadState(streamThread));
        }
    }

    @Override
    public void resume() {
        // resumes Thread
        // TODO :  throw an exception
        System.out.println("Thread has already running.");
    }
}
