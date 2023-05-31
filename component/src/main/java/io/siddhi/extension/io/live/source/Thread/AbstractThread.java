package io.siddhi.extension.io.live.source.Thread;

import io.siddhi.extension.io.live.utils.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractThread implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractThread.class);
    protected final Monitor pauseMonitor;
    protected volatile boolean isThreadRunning = true;
    protected volatile boolean isPaused = false;
    protected ThreadState threadState = new RunningThreadState(this);

    public AbstractThread(){
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
