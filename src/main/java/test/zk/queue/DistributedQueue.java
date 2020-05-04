package test.zk.queue;

import test.zk.lock.DistributedLock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

public class DistributedQueue {
    private ZooKeeper zk;
    private String rootPath;
    private DistributedLock distributedLock;

    public DistributedQueue(String lockKey) {
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
            distributedLock = new DistributedLock("LockForDistributedQueue_" + lockKey.trim());
            Stat stat = zk.exists(rootPath, false);
            if (stat == null) {
                try {
                    zk.create(rootPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (Exception e) {
                    if (!(e instanceof KeeperException.NodeExistsException)) {
                        throw new Exception(e);
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                return;
            }
            throw new RuntimeException(e);
        }
    }

    public int size() {
        try {
            return zk.getChildren(rootPath, null).size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean contains(String o) {
        if (o == null) {
            return false;
        }
        try {
            List<String> list = zk.getChildren(rootPath, null);
            for (String item : list) {
                byte[] bytes = zk.getData(rootPath + "/" + item, false, null);
                String value = new String(bytes, "utf-8");
                if (value.equals(o)) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public boolean add(String o) {
        try {
            zk.create(rootPath + "/", o.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean remove(String o) {
        if (o == null) {
            return false;
        }
        try {
            List<String> list = zk.getChildren(rootPath, null);
            for (String item : list) {
                byte[] bytes = zk.getData(rootPath + "/" + item, false, null);
                String value = new String(bytes, "utf-8");
                if (value.equals(o)) {
                    Stat stat = zk.exists(rootPath + "/" + item, null);
                    if (stat != null) {
                        zk.delete(rootPath + "/" + item, stat.getVersion());
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public void clear() {
        try {
            List<String> list = zk.getChildren(rootPath, null);
            for (String item : list) {
                Stat stat = zk.exists(rootPath + "/" + item, null);
                zk.delete(rootPath + "/" + item, stat.getVersion());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String poll() {
        try {
            distributedLock.lock();
            List<String> list = zk.getChildren(rootPath, null);
            if (list.isEmpty()) {
                return null;
            }
            TreeSet<String> set = new TreeSet<>();
            for (String item : list) {
                set.add(rootPath + "/" + item);
            }
            String first = set.first();
            String value = new String(zk.getData(first, false, null));
            zk.delete(first, 0);
            return value;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            distributedLock.unlock();
        }
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
