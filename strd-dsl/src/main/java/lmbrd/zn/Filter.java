package lmbrd.zn;

import lmbrd.zn.util.Hash;
import lmbrd.zn.util.PrimitiveBits;

import java.io.IOException;
import java.io.OutputStream;

/**
 * User: light
 * Date: 09/12/13
 * Time: 15:11
 */
public abstract class Filter {
      private static final byte[] VERSION = PrimitiveBits.shortToBytes(1);

      /** The vector size of <i>this</i> filter. */
      protected int vectorSize;

      /** The hash function used to map a key to several positions in the vector. */
      protected HashFunction hash;

      /** The number of hash function to consider. */
      protected int nbHash;

      /** Type of hashing function to use. */
      protected int hashType;

      protected Filter() {}

      /**
       * Constructor.
       * @param vectorSize The vector size of <i>this</i> filter.
       * @param nbHash The number of hash functions to consider.
       * @param hashType type of the hashing function (see {@link Hash}).
       */
      protected Filter(int vectorSize, int nbHash, int hashType) {
        this.vectorSize = vectorSize;
        this.nbHash = nbHash;
        this.hashType = hashType;
        this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
      }

      /**
       * Adds a key to <i>this</i> filter.
       * @param key The key to add.
       */
      public abstract void add(byte[] key);

      /**
       * Determines wether a specified key belongs to <i>this</i> filter.
       * @param key The key to test.
       * @return boolean True if the specified key belongs to <i>this</i> filter.
       *                      False otherwise.
       */
      public abstract boolean membershipTest(byte[] key);

      /**
       * Peforms a logical AND between <i>this</i> filter and a specified filter.
       * <p>
       * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
       * @param filter The filter to AND with.
       */
      public abstract void and(Filter filter);

      /**
       * Peforms a logical OR between <i>this</i> filter and a specified filter.
       * <p>
       * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
       * @param filter The filter to OR with.
       */
      public abstract void or(Filter filter);

      /**
       * Peforms a logical XOR between <i>this</i> filter and a specified filter.
       * <p>
       * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
       * @param filter The filter to XOR with.
       */
      public abstract void xor(Filter filter);

      /**
       * Performs a logical NOT on <i>this</i> filter.
       * <p>
       * The result is assigned to <i>this</i> filter.
       */
      public abstract void not();

    /*  *//**
       * Adds a list of keys to <i>this</i> filter.
       * @param keys The list of keys.
       *//*
      public void add(List<byte[]> keys){
        if(keys == null) {
          throw new IllegalArgumentException("ArrayList<byte[]> may not be null");
        }

        for(byte[] key: keys) {
          add(key);
        }
      }//end add()

      *//**
       * Adds a collection of keys to <i>this</i> filter.
       * @param keys The collection of keys.
       *//*
      public void add(Collection<byte[]> keys){
        if(keys == null) {
          throw new IllegalArgumentException("Collection<byte[]> may not be null");
        }
        for(byte[] key: keys) {
          add(key);
        }
      }//end add()

      *//**
       * Adds an array of keys to <i>this</i> filter.
       * @param keys The array of keys.
       *//*
      public void add(byte[][] keys){
        if(keys == null) {
          throw new IllegalArgumentException("byte[][] may not be null");
        }
        for(int i = 0; i < keys.length; i++) {
          add(keys[i]);
        }
      }//end add()
*/
      // Writable interface

      public void write(OutputStream out) throws IOException {
        out.write(VERSION);

        out.write(PrimitiveBits.shortToBytes(this.nbHash));

        out.write(this.hashType);
        out.write(PrimitiveBits.intToBytes(this.vectorSize));
      }

      public void read(PrimitiveBits.DataBuf in) throws IOException {
        int ver = in.readShort();

        if (ver == 1) {
          this.nbHash = in.readShort();
          this.hashType = in.readByte();
        } else {
          throw new IOException("Unsupported version: " + ver);
        }
        this.vectorSize = in.readInt();
        this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
      }
}
