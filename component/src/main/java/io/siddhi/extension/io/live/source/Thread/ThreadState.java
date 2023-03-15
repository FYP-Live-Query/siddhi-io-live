package io.siddhi.extension.io.live.source.Thread;

public abstract class ThreadState {

    protected AbstractThread thread;

    public ThreadState(AbstractThread thread) {
        this.thread = thread;
    }

    public void stop() {
        throw new UnsupportedOperationException("method not implemented in this state");
    }

    public  void pause(){
        throw new UnsupportedOperationException("method not implemented in this state");
    };

    public  void resume(){
        throw new UnsupportedOperationException("method not implemented in this state");
    };

}
