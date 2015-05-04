package lmbrd.io;

/**
 * User: light
 * Date: 30/01/14
 * Time: 15:52
 */
public class Allocator {
    public long allocate(int size) {
        throw new UnsupportedOperationException();
    }
    public synchronized void clear() {

    }
/*
    public static final AtomicLong allocated = new AtomicLong();

//    protected int position = 0;
    public static int PAGE_SIZE = JUnsafe.unsafe.pageSize() * 16;

    protected MemPage[] memPages = new MemPage[1];

    public Allocator() {
        memPages[0] = allocateNative(PAGE_SIZE);
    }



    public synchronized void release() {
        for (MemPage memPage : memPages) {
            allocated.addAndGet(-memPage.capacity);
            memPage.release();
        }
        memPages = null;
    }
    public synchronized void clear() {
        release();
        memPages = new MemPage[1];
        memPages[0] = allocateNative(PAGE_SIZE);
    }

    protected MemPage lastPage() {
        return memPages[memPages.length - 1];
    }


    public long allocate(int size) {
        MemPage memPage = lastPage();
        if (memPage.capacity - memPage.position < size) {
            memPage = grow(size);
        }

        return memPage.alloc(size);
    }


    private MemPage grow(int minimalSize) {
        System.out.println("Grow: " + minimalSize +" / " + lastPage().remaining());
        MemPage[] buffers1 = memPages;

        int lastBuf = buffers1.length;

        memPages = new MemPage[lastBuf + 1];
        System.arraycopy(buffers1, 0, memPages, 0, lastBuf);

        MemPage newPage = allocateNative( Math.max(PAGE_SIZE, minimalSize) );
        memPages[lastBuf] = newPage;

        return newPage;
    }

    private MemPage allocateNative(int max) {
        long offset = JUnsafe.unsafe.allocateMemory(max);
        allocated.addAndGet(max);
        return new MemPage(offset, max);
    }


    private int getGrowSize() {
        if (position < 64) {
            return position * 2;
        } else if (position < 256) {
            return (int) (position * 1.5);
        } else if (position < 1024) {
            return (int) (position * 1.1);
        } else {
            return (int) (position * 0.5);
        }
    }
*/

    //public
}
