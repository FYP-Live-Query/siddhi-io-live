package io.siddhi.extension.io.live.source.Thread;

import io.siddhi.extension.io.live.utils.Monitor;

public abstract class AbstractThread implements Runnable {
    protected final Monitor pauseMonitor;
    protected volatile boolean isThreadRunning = true;
    protected boolean isPaused;
    protected ThreadState threadState = new RunningThreadState(this);

    public AbstractThread(){
        this.isPaused = false;
        this.pauseMonitor = new Monitor();
    }


    public boolean isThreadRunning() {
        return isThreadRunning;
    }

    public void setThreadRunning(boolean threadRunning) {
        isThreadRunning = threadRunning;
    }

    public void stop() {
        this.threadState.stop();
    }

    public boolean isPaused() {
        return isPaused;
    }

    public void setPaused(boolean isPaused){
        this.isPaused = isPaused;
    }

    protected void doPause() {
        this.isPaused = true;
        synchronized(pauseMonitor){
            while(!pauseMonitor.isSignalled()){
                try{
                    pauseMonitor.wait();
                } catch(InterruptedException e){

                }
            }
            //clear signal and continue running.
            pauseMonitor.setSignalled(false);
        }
    }

    public void pause() {
        this.threadState.pause();
    }

    public void doResume() {
        synchronized(pauseMonitor){
            pauseMonitor.setSignalled(true);
            pauseMonitor.notify();
        }
    }

    public void resume() {
        this.threadState.resume();
    }

    public void setThreadState(ThreadState threadState) {
        this.threadState = threadState;
    }
}
