package strd.util;

import com.twitter.ostrich.stats.Stats;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * User: light
 * Date: 28/04/14
 * Time: 13:06
 */
public class ZkPooledIds {
    private final int poolSize;
    private final String path;
    private final DistributedAtomicLong zkCounter;
    private final ConcurrentLinkedQueue<Long> queue = new ConcurrentLinkedQueue<>();

    public ZkPooledIds(int poolSize, CuratorFramework curator, String path) {
        this.poolSize = poolSize;
        this.path = path;
        this.zkCounter = new DistributedAtomicLong(curator, path, new ExponentialBackoffRetry(100, 20));
    }

    public long getZk() throws Exception {
        return zkCounter.get().postValue();
    }

    public long fetch() throws Exception {
        Long value = queue.poll();
        if (value == null) {
            synchronized (this) {
                value = queue.poll();
                if (value == null) {
                    value = populatePool();
                }
            }
        }
        return value;
    }

    public void offer( long id ) {
        queue.add(id);
    }

    private long populatePool() throws Exception {
        Stats.incr("ids/" + path +"/consumed", poolSize);

        final AtomicValue<Long> atomic = zkCounter.add((long)poolSize);

        if ( atomic.succeeded() ) {
            final long max = atomic.postValue();
            final long start = max - poolSize;
            for (long id = start + 1; id < max; id++) {
                queue.add(id);
            }
            return start;
        } else {
            throw new IllegalStateException("unsuccessed atomic long update");
        }
    }
}
