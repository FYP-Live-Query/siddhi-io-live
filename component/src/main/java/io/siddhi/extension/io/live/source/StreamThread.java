package io.siddhi.extension.io.live.source;

import io.siddhi.core.stream.input.source.SourceEventListener;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class StreamThread implements Runnable {
    private PulsarClient pulsarClient;
    private String serviceUrlOfPulsarServer;
    private String topicOfStream;
    private String subscriptionNameOfConsumer;
    private Consumer consumer;
    private final Monitor signalMonitor;
    private final Monitor pauseMonitor;
    private volatile boolean isThreadRunning = true;
    private boolean isPaused;
    private ThreadState threadState = new RunningThreadState(this);
    private final SourceEventListener sourceEventListener;

    public StreamThread(String serviceUrlOfPulsarServer, String topicOfStream,
                        String subscriptionNameOfConsumer, Monitor signalMonitor, SourceEventListener sourceEventListener) {
        this.serviceUrlOfPulsarServer = serviceUrlOfPulsarServer;
        this.topicOfStream = topicOfStream;
        this.subscriptionNameOfConsumer = subscriptionNameOfConsumer;
        this.signalMonitor = signalMonitor;
        this.isPaused = false;
        this.pauseMonitor = new Monitor();
        this.sourceEventListener = sourceEventListener;
    }

    @Override
    public void run() {
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(serviceUrlOfPulsarServer).build();
            consumer = pulsarClient.newConsumer().topic(topicOfStream).subscriptionName(subscriptionNameOfConsumer)
                    .subscribe();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        while(isThreadRunning){
            Message msg = null;
            try {
                if(isPaused) {
                    System.out.println("paused - stream thread");
                    doPause();
                }
                msg = consumer.receive();sourceEventListener.onEvent(msg.getData(),null);
                String s = new String(msg.getData(), StandardCharsets.UTF_8);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
            try {
                consumer.acknowledge(msg);
            } catch (PulsarClientException e) {
                consumer.negativeAcknowledge(msg);
                e.printStackTrace();
            }
        }
    }

    // for thread signalling
    public void doWait(){
        synchronized(signalMonitor){
            while(!signalMonitor.isSignalled()){
                try{
                    signalMonitor.wait();
                } catch(InterruptedException e){

                }
            }
            //clear signal and continue running.
            signalMonitor.setSignalled(false);
        }
    }

    // for thread signalling
    public void doNotify(){
        synchronized(signalMonitor){
            signalMonitor.setSignalled(true);
            signalMonitor.notify();
        }
    }
    public boolean isThreadRunning() {
        return isThreadRunning;
    }

    public void setThreadRunning(boolean threadRunning) {
        isThreadRunning = threadRunning;
    }

    protected void stop() {
        this.threadState.stop();
    }

    public Monitor getPauseMonitor() {
        return pauseMonitor;
    }

    public boolean isPaused() {
        return isPaused;
    }

    public void setPaused(boolean isPaused){
        this.isPaused = isPaused;
    }

    private void doPause() {
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

    protected void pause() {
        this.threadState.pause();
    }

    protected void doResume() {
        synchronized(pauseMonitor){
            pauseMonitor.setSignalled(true);
            pauseMonitor.notify();
        }
    }

    protected void resume() {
        this.threadState.resume();
    }

    public void setThreadState(ThreadState threadState) {
        this.threadState = threadState;
    }
}
