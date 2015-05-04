package lmbrd.zn;

import lmbrd.zn.util.PrimitiveBits;

import java.io.IOException;
import java.io.OutputStream;
import java.util.BitSet;

/**
 * User: light
 * Date: 09/12/13
 * Time: 15:03
 */
public class BloomFilter extends Filter {

    private static final byte[] bitvalues = new byte[]{
            (byte) 0x01,
            (byte) 0x02,
            (byte) 0x04,
            (byte) 0x08,
            (byte) 0x10,
            (byte) 0x20,
            (byte) 0x40,
            (byte) 0x80
    };

    /**
     * The bit vector.
     */
    BitSet bits;

    private int uniqueCount = 0;

    /**
     * Default constructor - use with readFields
     */
    public BloomFilter() {
        super();
    }

    /**
     * Constructor
     *
     * @param vectorSize The vector size of <i>this</i> filter.
     * @param nbHash     The number of hash function to consider.
     * @param hashType   type of the hashing function (see
     *                   {@link org.apache.hadoop.util.hash.Hash}).
     */
    public BloomFilter(int vectorSize, int nbHash, int hashType) {
        super(vectorSize, nbHash, hashType);

        bits = new BitSet(this.vectorSize);
    }

    public int getSize() {
        return bits.cardinality();
    }

    @Override
    public void add(byte[] key) {
        if (key == null) {
            throw new NullPointerException("key cannot be null");
        }

        int[] h = hash.hash(key);
        hash.clear();

        for (int i = 0; i < nbHash; i++) {
            bits.set(h[i]);
        }
    }

    @Override
    public void and(Filter filter) {
        if (filter == null
                || !(filter instanceof BloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be and-ed");
        }

        this.bits.and(((BloomFilter) filter).bits);
    }

    @Override
    public boolean membershipTest(byte[] key) {
        if (key == null) {
            throw new NullPointerException("key cannot be null");
        }

        int[] h = hash.hash(key);
        hash.clear();
        for (int i = 0; i < nbHash; i++) {
            if (!bits.get(h[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void not() {
        bits.flip(0, vectorSize - 1);
    }

    @Override
    public void or(Filter filter) {
        if (filter == null
                || !(filter instanceof BloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be or-ed");
        }
        bits.or(((BloomFilter) filter).bits);
    }

    @Override
    public void xor(Filter filter) {
        if (filter == null
                || !(filter instanceof BloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be xor-ed");
        }
        bits.xor(((BloomFilter) filter).bits);
    }

    @Override
    public String toString() {
        return bits.toString();
    }

    /**
     * @return size of the the bloomfilter
     */
    public int getVectorSize() {
        return this.vectorSize;
    }

    // Writable

    @Override
    public void write(OutputStream out) throws IOException {
        super.write(out);
        byte[] bytes = new byte[getNBytes()];
        for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if (bitIndex == 0) {
                bytes[byteIndex] = 0;
            }
            if (bits.get(i)) {
                bytes[byteIndex] |= bitvalues[bitIndex];
            }
        }
        out.write(bytes);
    }

    @Override
    public void read(PrimitiveBits.DataBuf in) throws IOException {
        super.read(in);
        bits = new BitSet(this.vectorSize);

        byte[] bytes = in.readBytes(getNBytes());

        for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
                bits.set(i);
            }
        }
    }

    /* @return number of bytes needed to hold bit vector */
    private int getNBytes() {
        return (vectorSize + 7) / 8;
    }
}
