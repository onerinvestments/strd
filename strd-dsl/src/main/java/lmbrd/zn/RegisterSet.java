package lmbrd.zn;

import com.esotericsoftware.kryo.io.Input;
import lmbrd.zn.util.PrimitiveBits;

/**
 * User: light
 * Date: 09/12/13
 * Time: 16:27
 */
public class RegisterSet {
    public final static int LOG2_BITS_PER_WORD = 6;
    public final static int REGISTER_SIZE = 5;

    public final int count;
    public final int size;

    private final int[] M;

    public RegisterSet(int count) {
        this(count, null);
    }

    private RegisterSet( int count, int size, int mLength ) {
        this.count = count;
        this.size = size;
        this.M = new int[mLength];
    }

    public byte[] write() {

        byte[] array = new byte[ M.length * 4 + 12];

        int idx = 0;
        idx = PrimitiveBits.putInt(array, idx, count);
        idx = PrimitiveBits.putInt(array, idx, size);
        idx = PrimitiveBits.putInt(array, idx, M.length);

        for (int m : M) {
            PrimitiveBits.putInt(array,idx,m);
            idx += 4;
        }
        return array;
    }

    public int sizeInBytes() {
        return M.length * 4 + 12;
    }


    public static RegisterSet read(PrimitiveBits.DataBuf buf) {
        RegisterSet registerSet = new RegisterSet(buf.readInt(), buf.readInt(), buf.readInt());
        int length = registerSet.M.length;
        for (int  i =0 ; i < length; i++) {
            registerSet.M[i] = buf.readInt();
        }

        return registerSet;
    }

    public static RegisterSet read(Input buf) {
        RegisterSet registerSet = new RegisterSet(buf.readInt(), buf.readInt(), buf.readInt());
        int length = registerSet.M.length;
        for (int  i =0 ; i < length; i++) {
            registerSet.M[i] = buf.readInt();
        }

        return registerSet;
    }

    public RegisterSet(int count, int[] initialValues) {
        this.count = count;
        int bits = getBits(count);

        if (initialValues == null) {
            if (bits == 0) {
                this.M = new int[1];
            } else if (bits % Integer.SIZE == 0) {
                this.M = new int[bits];
            } else {
                this.M = new int[bits + 1];
            }
        } else {
            this.M = initialValues;
        }
        this.size = this.M.length;
    }

    public static int getBits(int count) {
        return count / LOG2_BITS_PER_WORD;
    }

    public void set(int position, int value) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        this.M[bucketPos] = (this.M[bucketPos] & ~(0x1f << shift)) | (value << shift);
    }

    public int get(int position) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.M[bucketPos] & (0x1f << shift)) >>> shift;
    }

    public boolean updateIfGreater(int position, int value) {
        int bucket = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
        int mask = 0x1f << shift;

        // Use long to avoid sign issues with the left-most shift
        long curVal = this.M[bucket] & mask;
        long newVal = value << shift;
        if (curVal < newVal) {
            this.M[bucket] = (int) ((this.M[bucket] & ~mask) | newVal);
            return true;
        } else {
            return false;
        }
    }

    public void merge(RegisterSet that) {
        for (int bucket = 0; bucket < M.length; bucket++) {
            int word = 0;
            for (int j = 0; j < LOG2_BITS_PER_WORD; j++) {
                int mask = 0x1f << (REGISTER_SIZE * j);

                int thisVal = (this.M[bucket] & mask);
                int thatVal = (that.M[bucket] & mask);
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }
            this.M[bucket] = word;
        }
    }

    int[] readOnlyBits() {
        return M;
    }

    public int[] bits() {
        int[] copy = new int[size];
        System.arraycopy(M, 0, copy, 0, M.length);
        return copy;
    }

}
