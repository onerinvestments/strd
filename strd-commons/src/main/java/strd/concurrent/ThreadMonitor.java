package strd.concurrent;

import lmbrd.zn.util.JUnsafe;
import sun.misc.Unsafe;

/**
 * User: light
 * Date: 7/29/13
 * Time: 4:13 PM
 */
public class ThreadMonitor {



    /// padding for cacheline
    public int  p7 = 1;
    public long p1, p2, p3 = 2L;
    public int workerThread = -1;
    public long p4 ,p5, p6 = 7L;
    /// padding for cacheline

    private static Unsafe unsafe = JUnsafe.unsafe;
    private static final long workerThreadOffs;

    static {
        try {
            workerThreadOffs = unsafe.objectFieldOffset(ThreadMonitor.class.getDeclaredField("workerThread"));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }


    public boolean cas(int expected, int toSet) {
        return unsafe.compareAndSwapInt(this, workerThreadOffs, expected, toSet);
    }

    public int volatileRead() {
        return unsafe.getIntVolatile(this, workerThreadOffs);
    }

    public void volatileWrite(int val) {
        unsafe.putIntVolatile(this, workerThreadOffs, val);
    }

    public long __et() {
        return p1 + p2 + p3 + p4 + p5 + p6 + p7;
    }

    public boolean tryAcquire(int fromThread) {
        return workerThread == -1 && cas(-1, fromThread);
    }

    public void release(int fromThread) {
        if ( workerThread != fromThread ) {
            throw new IllegalStateException();
        }

        volatileWrite( -1);
    }
}
