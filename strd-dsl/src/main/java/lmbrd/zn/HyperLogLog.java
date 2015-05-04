package lmbrd.zn;

import com.esotericsoftware.kryo.io.Input;
import lmbrd.zn.util.MurmurHash;
import lmbrd.zn.util.PrimitiveBits;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * User: light
 * Date: 09/12/13
 * Time: 16:26
 */
public class HyperLogLog {
    public final RegisterSet registerSet;
    private final int log2m;
    private final double alphaMM;


    /**
     * Create a new HyperLogLog instance using the specified standard deviation.
     *
     * @param rsd - the relative standard deviation for the counter.
     *            smaller values create counters that require more space.
     */
    public HyperLogLog(double rsd) {
        this(log2m(rsd));
    }

    private static int log2m(double rsd) {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    /**
     * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
     * the counter.  The larger the log2m the better the accuracy.
     * <p/>
     * accuracy = 1.04/sqrt(2^log2m)
     *
     * @param log2m - the number of bits to use as the basis for the HLL instance
     */
    public HyperLogLog(int log2m) {
        this(log2m, new RegisterSet(1 << log2m));
    }

    /**
     * Creates a new HyperLogLog instance using the given registers.  Used for unmarshalling a serialized
     * instance and for merging multiple counters together.
     *
     * @param registerSet - the initial values for the register set
     */
    public HyperLogLog(int log2m, RegisterSet registerSet) {
        if (log2m < 0 || log2m > 30) {
            throw new IllegalArgumentException("log2m argument is "
                    + log2m + " and is outside the range [0, 30]");
        }
        this.registerSet = registerSet;
        this.log2m = log2m;
        int m = 1 << this.log2m;

        // See the paper.
        switch (log2m) {
            case 4:
                alphaMM = 0.673 * m * m;
                break;
            case 5:
                alphaMM = 0.697 * m * m;
                break;
            case 6:
                alphaMM = 0.709 * m * m;
                break;
            default:
                alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }


    public boolean offerHashed(long hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = (int) (hashedValue >>> (Long.SIZE - log2m));
        final int r = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }


    public boolean offerHashed(int hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = hashedValue >>> (Integer.SIZE - log2m);
        final int r = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }


    public boolean offer(byte[] o) {
        final int x = MurmurHash.getInstance().hash(o);
        return offerHashed(x);
    }


    public long cardinality() {
        double registerSum = 0;
        int count = registerSet.count;
        double zeros = 0.0;
        for (int j = 0; j < registerSet.count; j++) {
            int val = registerSet.get(j);
            registerSum += 1.0 / (1 << val);
            if (val == 0) {
                zeros++;
            }
        }

        double estimate = alphaMM * (1 / registerSum);

        if (estimate <= (5.0 / 2.0) * count) {
            // Small Range Estimate
            return Math.round(count * Math.log(count / zeros));
        } else {
            return Math.round(estimate);
        }
    }


    public int sizeof() {
        return registerSet.size * 4;
    }


    public void write(OutputStream os) throws IOException {

        os.write(PrimitiveBits.intToBytes(log2m));
        os.write(registerSet.write());

    }

    public int sizeInBytes() {
        return 4 + registerSet.sizeInBytes();
    }


    public static HyperLogLog read( PrimitiveBits.DataBuf buf ) {
        return new HyperLogLog( buf.readInt(), RegisterSet.read(buf));
    }
    public static HyperLogLog read( Input buf ) {
        return new HyperLogLog( buf.readInt(), RegisterSet.read(buf));
    }


    /**
     * Add all the elements of the other set to this set.
     * <p/>
     * This operation does not imply a loss of precision.
     *
     * @param other A compatible Hyperloglog instance (same log2m)
     * @throws CardinalityMergeException if other is not compatible
     */
    public void addAll(HyperLogLog other) {
        if (this.sizeof() != other.sizeof()) {
            throw new HyperLogLogMergeException("Cannot merge estimators of different sizes");
        }

        registerSet.merge(other.registerSet);
    }


    public HyperLogLog merge(HyperLogLog... estimators) {
        HyperLogLog merged = new HyperLogLog(log2m);
        merged.addAll(this);

        if (estimators == null) {
            return merged;
        }

        for (HyperLogLog estimator : estimators) {
            merged.addAll(estimator);
        }

        return merged;
    }

    public static class Builder {
        private double rsd;

        public Builder(double rsd) {
            this.rsd = rsd;
        }

        public HyperLogLog build() {
            return new HyperLogLog(rsd);
        }

        public int sizeof() {
            int log2m = log2m(rsd);
            int k = 1 << log2m;
            return RegisterSet.getBits(k) * 4;
        }

        public static HyperLogLog build(byte[] bytes) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            DataInputStream oi = new DataInputStream(bais);
            int log2m = oi.readInt();
            int size = oi.readInt();
            byte[] longArrayBytes = new byte[size];
            oi.readFully(longArrayBytes);
            return new HyperLogLog(log2m, new RegisterSet(1 << log2m, Bits.getBits(longArrayBytes)));
        }
    }

    @SuppressWarnings("serial")
    protected static class HyperLogLogMergeException extends RuntimeException {
        public HyperLogLogMergeException(String message) {
            super(message);
        }
    }

    public static class Bits {

        public static int[] getBits(byte[] mBytes) throws IOException {
            int bitSize = mBytes.length / 4;
            int[] bits = new int[bitSize];
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(mBytes));
            for (int i = 0; i < bitSize; i++) {
                bits[i] = dis.readInt();
            }
            return bits;
        }

    }
}
