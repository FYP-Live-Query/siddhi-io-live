package io.siddhi.extension.io.live.source.Thread;

public abstract class ThreadState {

    protected AbstractThread thread;

    public ThreadState(AbstractThread thread) {
        this.thread = thread;
    }

    public abstract void stop();
    public abstract void pause();
    public abstract void resume();

}
