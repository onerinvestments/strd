package lmbrd.zn;

import lmbrd.zn.util.Hash;

/**
 * User: light
 * Date: 09/12/13
 * Time: 15:12
 */
public class HashFunction {
    /**
     * The number of hashed values.
     */
    private int nbHash;

    /**
     * The maximum highest returned value.
     */
    private int maxValue;

    /**
     * Hashing algorithm to use.
     */
    private Hash hashFunction;

    /**
     * Constructor.
     * <p/>
     * Builds a hash function that must obey to a given maximum number of returned values and a highest value.
     *
     * @param maxValue The maximum highest returned value.
     * @param nbHash   The number of resulting hashed values.
     * @param hashType type of the hashing function (see {@link Hash}).
     */
    public HashFunction(int maxValue, int nbHash, int hashType) {
        if (maxValue <= 0) {
            throw new IllegalArgumentException("maxValue must be > 0");
        }

        if (nbHash <= 0) {
            throw new IllegalArgumentException("nbHash must be > 0");
        }

        this.maxValue = maxValue;
        this.nbHash = nbHash;
        this.hashFunction = Hash.getInstance(hashType);
        if (this.hashFunction == null)
            throw new IllegalArgumentException("hashType must be known");
    }

    /**
     * Clears <i>this</i> hash function. A NOOP
     */
    public void clear() {
    }

    /**
     * Hashes a specified key into several integers.
     *
     * @param k The specified key.
     * @return The array of hashed values.
     */
    public int[] hash(byte[] b) {
        if (b == null) {
            throw new NullPointerException("buffer reference is null");
        }
        if (b.length == 0) {
            throw new IllegalArgumentException("key length must be > 0");
        }
        int[] result = new int[nbHash];
        for (int i = 0, initval = 0; i < nbHash; i++) {
            initval = hashFunction.hash(b, initval);
            result[i] = Math.abs(initval % maxValue);
        }
        return result;
    }
}
