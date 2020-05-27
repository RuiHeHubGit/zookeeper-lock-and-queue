package test.zk.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DistributedLock {
    private static final byte[] bytes = {};
    private ZooKeeper zk;
    private String rootPath;
    private ThreadLocal<String> currentLock;
    private ThreadLocal<String> lastNode;

    public DistributedLock(String lockKey) {
        String connectString = System.getProperty("distributed.lock.zk.connect.string", "localhost:2181");
        if (lockKey == null || lockKey.trim().isEmpty()) {
            rootPath = "/default_lock_path";
        }
        if (lockKey.indexOf("/") > 0) {
            throw new IllegalArgumentException("invalid key for lock:" + lockKey);
        }

        rootPath = lockKey.trim();
        if (!rootPath.startsWith("/")) {
            rootPath = "/" + rootPath;
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            zk = new ZooKeeper(connectString, 60000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                        countDownLatch.countDown();
                    }
                }
            });

            countDownLatch.await();
            currentLock = ThreadLocal.withInitial(() -> {
                try {
                    return zk.create(rootPath + "/", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, null);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            lastNode = ThreadLocal.withInitial(() -> null);
            Stat stat = zk.exists(rootPath, false);
            if (stat == null) {
                zk.create(rootPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER);
            }
        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                return;
            }
            throw new RuntimeException(e);
        }
    }

    public void lock() {
        if (tryLock(true)) {
            return;
        }
        while(true) {
            try {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                zk.addWatch(lastNode.get(), new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        countDownLatch.countDown();
                    }
                }, AddWatchMode.PERSISTENT);
                if (tryLock(true)) {
                    return;
                }
                countDownLatch.await();
                if (tryLock(true)) {
                    return;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean tryLock() {
        return tryLock(false);
    }

    private boolean tryLock(boolean lock) {
        try {
            currentLock.get();
            List<String> children = zk.getChildren(rootPath, false);
            TreeSet<String> sortedChildren = new TreeSet<String>();
            for (String child : children) {
                sortedChildren.add(rootPath + "/" + child);
            }
            String fistChild = sortedChildren.first();
            if (fistChild.equals(currentLock.get())) {
                return true;
            }
            if (lock) {
                SortedSet<String> beforeChildren = sortedChildren.headSet(currentLock.get());
                lastNode.set(beforeChildren.last());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public boolean tryLock(long time, TimeUnit unit) {
        long endTime = System.currentTimeMillis() + unit.toMillis(time);
        while (System.currentTimeMillis() <= endTime) {
            if (tryLock(false)) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
        }
        return false;
    }

    public void unlock() {
        if (currentLock != null) {
            try {
                Stat stat = zk.exists(currentLock.get(), null);
                zk.delete(currentLock.get(), stat.getVersion());
                currentLock.remove();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
