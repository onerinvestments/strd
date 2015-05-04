package lmbrd.zn.util;

import lmbrd.io.JVMReservedMemory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

/**
 * User: light
 * Date: 07/11/13
 * Time: 14:01
 */
public class ByteBuf extends OutputStream {
    //    private int sliceCapacity = 0;
    protected int position = 0;
    //    private int current = 0;
    protected boolean readonly = false;

    protected ByteBuffer[] buffers = new ByteBuffer[1];

    public ByteBuf(int initial) {
        buffers[0] = allocate(initial);
    }

    protected ByteBuf() {
    }

    public void grow(int minimalSize) {
        ByteBuffer[] buffers1 = buffers;

        int lastBuf = buffers1.length;
        buffers[lastBuf - 1].flip();

        buffers = new ByteBuffer[lastBuf + 1];
        System.arraycopy(buffers1, 0, buffers, 0, lastBuf);

        buffers[lastBuf] = allocate(Math.max(getGrowSize(), minimalSize));
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

    public ByteBuffer[] getBuffers() {
        setReadonly();
        return buffers;
    }

    public ByteBuffer[] getBuffersDirty() {
        return buffers;
    }

    protected ByteBuffer allocate(int size) {
        return JVMReservedMemory.buffer(size);
    }


    private void setReadonly() {
        if (! readonly) {
            lastBuffer().flip();
        }

        readonly = true;
    }

    @Override
    public void write(int b) throws IOException {
        checkWrite();
        ByteBuffer buffer = lastBuffer();
        int bufferAvail = buffer.capacity() - buffer.position();
        if (bufferAvail == 0) {
            grow(1);
            buffer = lastBuffer();
        }

        position++;
        buffer.put((byte) b);
    }

    public void writeInt(int b) throws IOException {
        checkWrite();
        ByteBuffer buffer = lastBuffer();
        int bufferAvail = buffer.capacity() - buffer.position();
        if (bufferAvail > 0 && bufferAvail < 4) {  // write partial int(4) bytes
            write(PrimitiveBits.intToBytes(b));
        } else {
            if (bufferAvail == 0) {
                grow(4);
                buffer = lastBuffer();
            }

            buffer.putInt(b);
            position += 4;
        }
    }

    public void writeLong(long b) throws IOException {
        checkWrite();
        ByteBuffer buffer = lastBuffer();
        int bufferAvail = buffer.capacity() - buffer.position();
        if (bufferAvail > 0 && bufferAvail < 8) {  // write partial long(8) bytes
            write(PrimitiveBits.longToBytes(b));
        } else {
            if (bufferAvail == 0) {
                grow(8);
                buffer = lastBuffer();
            }

            buffer.putLong(b);
            position += 8;
        }
    }


    protected ByteBuffer lastBuffer() {
        return buffers[buffers.length - 1];
    }

    @Override
    public void write(final byte[] b, final int off1, final int len1) {
        checkWrite();

        int len = len1;
        position += len;

        int off = off1;

        while (len > 0) {
            ByteBuffer buffer = lastBuffer();

            int bufferAvail = buffer.capacity() - buffer.position();
            if (bufferAvail == 0) {
                grow(len);
            } else {
                int toWrite = Math.min(len, bufferAvail);

                buffer.put(b, off, toWrite);

                off += toWrite;
                len -= toWrite;
            }
        }
    }

    public void write(final ByteBuffer b) {
        checkWrite();

        int len = b.remaining();
        position += len;

        int off = b.position();

        while (len > 0) {
            ByteBuffer buffer = lastBuffer();

            int bufferAvail = buffer.capacity() - buffer.position();
            if (bufferAvail == 0) {
                grow(len);
            } else {
                int toWrite = Math.min(len, bufferAvail);

                int pp = b.position();
                b.position(off);

                ByteBuffer slice = b.slice();
                slice.limit( toWrite );

                b.position(pp);

                buffer.put(slice);

                off += toWrite;
                len -= toWrite;
            }
        }
    }


    private void checkWrite() {
        if (readonly) {
            throw new IllegalStateException("readonly");
        }
    }

    public synchronized void cleanup(int bufferIdx) {
        if (buffers[bufferIdx] == null) {
            return;
        }

        JVMReservedMemory.cleanBuffer(buffers[bufferIdx]);
        buffers[bufferIdx] = null;
    }

    public int getWrittenBytes(ByteBuffer buffer) {
        int l = buffer.limit();
        int c = buffer.capacity();
        int p = buffer.position();

        if (p == 0) {
            return l;
        } else {
            return p;
        }
    }


    public void replaceInt(int off1, int value) {
        int o = 0;

        for (ByteBuffer buffer : buffers) {

            int w = getWrittenBytes(buffer);

            if ((o + w) > off1) {
                int bufferOffset = off1 - o;
                buffer.putInt(bufferOffset, value);
                return;
            }
        }

        throw new BufferOverflowException();
    }

    public int position() {
        return position;
    }

}
