package com.tietoevry.datadog;

import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SendingThread extends Thread {
    private final BlockingQueue<Message> queue;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final Consumer<List> sendMessage;
    private final ExecutorService executorService;
    private final Semaphore semaphore;
    private final Logger log = LoggerFactory.getLogger(SendingThread.class);


    public SendingThread(int connections, BlockingQueue<Message> queue, Consumer<List> sendMessages) {
        this.queue = queue;
        this.sendMessage = sendMessages;

        this.executorService = Executors.newFixedThreadPool(connections);
        this.semaphore = new Semaphore(connections);
    }

    public void stopThread() {
        isRunning.set(false);
        executorService.shutdown();
    }

    public synchronized void notifyThread() {
        this.notify();
    }

    @Override
    public void run() {
        while (isRunning.get()) {
            synchronized (this) {
                try {
                    if (queue.size() > 0) {
                        ArrayList<Message> data = new ArrayList<>();
                        queue.drainTo(data);

                        semaphore.acquire();
                        executorService.execute(() -> {
                            sendMessage.accept(data);
                            semaphore.release();
                        });
                    }
                    this.wait();
                } catch (InterruptedException e) {
                    log.error("Interrupted sending thread", e);
                }
            }
        }
    }
}
