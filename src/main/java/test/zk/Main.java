package test.zk;

import test.zk.lock.DistributedLock;
import test.zk.queue.DistributedQueue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        testDistributedLock();
        Thread.sleep(25000);
        testDistributedQueue();
    }

    private static void testDistributedQueue() {
        System.out.println("test queue");
        int count = 10;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            int index = i;
            new Thread(() -> {
                DistributedQueue distributedQueue = new DistributedQueue("queue1");
                distributedQueue.add("data-" + index);
                distributedQueue.close();
                countDownLatch.countDown();
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        AtomicInteger atomicInteger = new AtomicInteger();
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                DistributedQueue distributedQueue = new DistributedQueue("queue1");
                String value = distributedQueue.poll();
                System.out.println(atomicInteger.getAndIncrement() + ":" + value);
                distributedQueue.close();
            }).start();
        }
    }

    private static void testDistributedLock() {
        System.out.println("test lock");
        int processCount = 5;
        CountDownLatch countDownLatch = new CountDownLatch(processCount);
        for (int i = 0; i < processCount; i++) {
            new Thread(() -> {
                countDownLatch.countDown();
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // a process
                DistributedLock lock = new DistributedLock("app1");
                for (int j = 0; j < 2; j++) {
                    // a thread for current process
                    new Thread(() -> {
                        lock.lock();
                        System.out.println(Thread.currentThread().getName() + " got lock for app1");
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        lock.unlock();
                    }).start();
                }
            }).start();
        }
    }
}
