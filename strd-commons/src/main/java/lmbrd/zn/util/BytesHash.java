package lmbrd.zn.util;

import gnu.trove.strategy.HashingStrategy;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Iterator;

/**
 * $Id$
 * $URL$
 * User: light
 * Date: 8/30/11
 * Time: 5:58 PM
 */
public class BytesHash implements HashingStrategy<byte[]> {
    public static byte[] MAX_512 = new byte[512];

    static {
        for (int i =0; i< 512; i++) {
            MAX_512[i] = (byte) 0xFF;
        }
    }

    public final static BytesHash instance = new BytesHash();

    final static Hash hashInstance = MurmurHash.getInstance();

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static final Comparator<byte[]>BYTES_COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return compareTo(o1, o2);
        }
    };

    @Override
    public int computeHashCode(byte[] array) {
        return hashInstance.hash(array);
    }


    public int computeHashCode(byte[] array, int offset, int length) {
        return hashInstance.hash(array, offset, length, -1);
    }

    /**
     * @param left  left operand
     * @param right right operand
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    public static int compareTo(final byte[] left, final byte[] right) {
        return compareTo(left, 0, left.length, right, 0, right.length);
    }

    /**
     * Lexographically compare two arrays.
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @param offset1 Where to start comparing in the left buffer
     * @param offset2 Where to start comparing in the right buffer
     * @param length1 How much to compare from the left buffer
     * @param length2 How much to compare from the right buffer
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    public static int compareTo(byte[] buffer1, int offset1, int length1,
                                byte[] buffer2, int offset2, int length2) {
        // Bring WritableComparator code local
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (buffer1[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }

    /**
     * @param left  left operand
     * @param right right operand
     * @return True if equal
     */
    public boolean equals(final byte[] left, final byte[] right) {
        // Could use Arrays.equals?
        //noinspection SimplifiableConditionalExpression
        return left == null && right == null || (left == null || right == null || (left.length != right.length) ? false : compareTo(left, right) == 0);
    }

    // TODO: выгоднее сравнивать с конца в начало


    /**
     * @param a      array
     * @param length amount of bytes to grab
     * @return First <code>length</code> bytes from <code>a</code>
     */
    public static byte[] head(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, 0, result, 0, length);
        return result;
    }

    /**
     * @param a      array
     * @param length amount of bytes to snarf
     * @return Last <code>length</code> bytes from <code>a</code>
     */
    public static byte[] tail(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, a.length - length, result, 0, length);
        return result;
    }

    /**
     * @param a      array
     * @param length new array size
     * @return Value in <code>a</code> plus <code>length</code> prepended 0 bytes
     */
    public static byte[] padHead(final byte[] a, final int length) {
        byte[] padding = new byte[length];
        for (int i = 0; i < length; i++) {
            padding[i] = 0;
        }
        return add(padding, a);
    }

    /**
     * @param a      array
     * @param length new array size
     * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
     */
    public static byte[] padTail(final byte[] a, final int length) {
        byte[] padding = new byte[length];
        for (int i = 0; i < length; i++) {
            padding[i] = 0;
        }
        return add(a, padding);
    }


    /**
     * @param a lower half
     * @param b upper half
     * @return New array that has a in lower half and b in upper half.
     */
    public static byte[] add(final byte[] a, final byte[] b) {
        return add(a, b, EMPTY_BYTE_ARRAY);
    }

    /**
     * @param a first third
     * @param b second third
     * @param c third third
     * @return New array made from a, b and c
     */
    public static byte[] add(final byte[] a, final byte[] b, final byte[] c) {
        byte[] result = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        return result;
    }

    /**
     * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
     * Useful splitting ranges for MapReduce jobs.
     *
     * @param a   Beginning of range
     * @param b   End of range
     * @param num Number of times to split range.  Pass 1 if you want to split
     *            the range in two; i.e. one split.
     * @return Array of dividing values
     */
    public static byte[][] split(final byte[] a, final byte[] b, final int num) {
        byte[][] ret = new byte[num + 2][];
        int i = 0;
        Iterable<byte[]> iter = iterateOnSplits(a, b, num);
        if (iter == null) return null;
        for (byte[] elem : iter) {
            ret[i++] = elem;
        }
        return ret;
    }

    /**
     * Iterate over keys within the passed inclusive range.
     */
    public static Iterable<byte[]> iterateOnSplits(
            final byte[] a, final byte[] b, final int num) {
        byte[] aPadded;
        byte[] bPadded;
        if (a.length < b.length) {
            aPadded = padTail(a, b.length - a.length);
            bPadded = b;
        } else if (b.length < a.length) {
            aPadded = a;
            bPadded = padTail(b, a.length - b.length);
        } else {
            aPadded = a;
            bPadded = b;
        }
        if (compareTo(aPadded, bPadded) >= 0) {
            throw new IllegalArgumentException("b <= a");
        }
        if (num <= 0) {
            throw new IllegalArgumentException("num cannot be < 0");
        }
        byte[] prependHeader = {1, 0};
        final BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
        final BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
        final BigInteger diffBI = stopBI.subtract(startBI);
        final BigInteger splitsBI = BigInteger.valueOf(num + 1);
        if (diffBI.compareTo(splitsBI) < 0) {
            return null;
        }
        final BigInteger intervalBI;
        try {
            intervalBI = diffBI.divide(splitsBI);
        } catch (Exception e) {
            throw new RuntimeException("Exception caught during division", e);
        }

        final Iterator<byte[]> iterator = new Iterator<byte[]>() {
            private int i = -1;

            @Override
            public boolean hasNext() {
                return i < num + 1;
            }

            @Override
            public byte[] next() {
                i++;
                if (i == 0) return a;
                if (i == num + 1) return b;

                BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
                byte[] padded = curBI.toByteArray();
                if (padded[1] == 0)
                    padded = tail(padded, padded.length - 2);
                else
                    padded = tail(padded, padded.length - 1);
                return padded;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };

        return new Iterable<byte[]>() {
            @Override
            public Iterator<byte[]> iterator() {
                return iterator;
            }
        };
    }


}
