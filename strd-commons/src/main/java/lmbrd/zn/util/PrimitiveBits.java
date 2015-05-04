package lmbrd.zn.util;

import org.jboss.netty.util.CharsetUtil;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.GZIPOutputStream;

/**
 * $Id$
 * $URL$
 * User: light
 * Date: Jun 26, 2009
 * Time: 3:02:00 PM
 * <p/>
 * Big-Endian
 */
public class PrimitiveBits {
    public static GZIPOutputStream gzipOutputStream(OutputStream out, Boolean sync) {
        try {
            return new GZIPOutputStream(out, sync);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }




    public static byte[] merge(byte[] b1, byte[] b2) {
        int split = b1.length;
        int v2l = b2.length;

        byte[] b = new byte[split + v2l];
        System.arraycopy(b1, 0, b, 0, split);
        System.arraycopy(b2, 0, b, split, v2l);
        return b;
    }


    public static byte[] slice(byte[] b, int off, int len) {
        byte[] buf = new byte[len];
        System.arraycopy(b, off, buf, 0, len );
        return buf;
    }

    public static final byte[] EMPTY = new byte[0];

    public static final Charset utf8 = CharsetUtil.UTF_8;

    /**
     * Size of boolean in bytes
     */
    public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

    /**
     * Size of byte in bytes
     */
    public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

    /**
     * Size of char in bytes
     */
    public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

    /**
     * Size of double in bytes
     */
    public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

    /**
     * Size of float in bytes
     */
    public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

    /**
     * Size of int in bytes
     */
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    /**
     * Size of long in bytes
     */
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    /**
     * Size of short in bytes
     */
    public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;



    public static byte[] readPrefixedArray( DataInputStream dis ) throws IOException {
        int len = dis.readShort();
        if (len < 0) {
            throw new IllegalStateException("array length < 0: " + len);
        }
        byte[] arr = new byte[len];

        if ( dis.read(arr) != len ) {
            throw new IllegalStateException();
        }

        return arr;
    }

    public static void writePrefixedArray(byte[] arr,
                                          DataOutputStream dos ) throws IOException {
        writePrefixedArray(arr, 0, arr.length, dos);
    }

    public static void writeIntPrefixedArray(byte[] arr,
                                          DataOutputStream dos ) throws IOException {
        dos.writeInt( arr.length );
        dos.write(arr);
    }

    public static byte[] readIntPrefixedArray(DataInputStream dis)throws IOException {
        int length = dis.readInt();
        byte[] arr = new byte[length];
        if ( dis.read(arr) != length) {
            throw new IllegalStateException();
        }
        return arr;
    }

    public static void writePrefixedArray(byte[] arr,
                                          int off,
                                          int len,
                                          DataOutputStream dos ) throws IOException {

        if (len > Short.MAX_VALUE -1) {
            throw new IllegalStateException("array too long");
        }

        dos.writeShort( len );
        dos.write(arr, off, len);
    }

	public static boolean getBit(long state, int bitIndex) {
		return (state & (1 << bitIndex)) != 0;
	}

	public static long setBit(long state, int bitIndex, boolean value) {
		if ( value ) {
			return state | (1 << bitIndex);
		} else {
			return state & ~(1 << bitIndex);
		}
	}

   /*
    * Methods for unpacking primitive values from byte arrays starting at
    * given offsets.
    */
	public static boolean getBoolean(byte[] b, int off) {
		return b[ off ] != 0;
	}

	public static char getChar(byte[] b, int off) {
		return (char) (((b[ off + 1 ] & 0xFF) << 0) +
				((b[ off + 0 ]) << 8));
	}

	public static short getShort(byte[] b, int off) {
		return (short) (((b[ off + 1 ] & 0xFF) << 0) +
				((b[ off + 0 ]) << 8));
	}

	public static int getInt(byte[] b, int off) {
		return ((b[ off + 3 ] & 0xFF) << 0) +
				((b[ off + 2 ] & 0xFF) << 8) +
				((b[ off + 1 ] & 0xFF) << 16) +
				((b[ off + 0 ]) << 24);
	}

	public static float getFloat(byte[] b, int off) {
		int i = ((b[ off + 3 ] & 0xFF) << 0) +
				((b[ off + 2 ] & 0xFF) << 8) +
				((b[ off + 1 ] & 0xFF) << 16) +
				((b[ off + 0 ]) << 24);
		return Float.intBitsToFloat( i );
	}

	public static long getLong(byte[] b, int off) {
		return ((b[ off + 7 ] & 0xFFL) << 0) +
				((b[ off + 6 ] & 0xFFL) << 8) +
				((b[ off + 5 ] & 0xFFL) << 16) +
				((b[ off + 4 ] & 0xFFL) << 24) +
				((b[ off + 3 ] & 0xFFL) << 32) +
				((b[ off + 2 ] & 0xFFL) << 40) +
				((b[ off + 1 ] & 0xFFL) << 48) +
				(((long) b[ off + 0 ]) << 56);
	}

	public static double getDouble(byte[] b, int off) {
		long j = ((b[ off + 7 ] & 0xFFL) << 0) +
				((b[ off + 6 ] & 0xFFL) << 8) +
				((b[ off + 5 ] & 0xFFL) << 16) +
				((b[ off + 4 ] & 0xFFL) << 24) +
				((b[ off + 3 ] & 0xFFL) << 32) +
				((b[ off + 2 ] & 0xFFL) << 40) +
				((b[ off + 1 ] & 0xFFL) << 48) +
				(((long) b[ off + 0 ]) << 56);
		return Double.longBitsToDouble( j );
	}

	/*
		* Methods for packing primitive values into byte arrays starting at given
		* offsets.
		*/

	public static int putBoolean(byte[] b, int off, boolean val) {
		b[ off ] = (byte) (val ? 1 : 0);
		return off + 1;
	}

	public static int putChar(byte[] b, int off, char val) {
		b[ off + 1 ] = (byte) (val >>> 0);
		b[ off + 0 ] = (byte) (val >>> 8);

		return off + 2;
	}

	public static int putShort(byte[] b, int off, short val) {
		b[ off + 1 ] = (byte) (val >>> 0);
		b[ off + 0 ] = (byte) (val >>> 8);

		return off + 2;
	}

	public static int putInt(byte[] b, int off, int val) {
		b[ off + 3 ] = (byte) (val >>> 0);
		b[ off + 2 ] = (byte) (val >>> 8);
		b[ off + 1 ] = (byte) (val >>> 16);
		b[ off + 0 ] = (byte) (val >>> 24);
		return off + 4;
	}

	public static int putBytes(byte[] dest, int destOff, byte[] src, int srcOff, int len) {
		System.arraycopy( src, srcOff, dest, destOff, len );
		return destOff + len;
	}

	public static int putBytes(byte[] dest, int destOff, byte[] src) {
		int len = src.length;
		System.arraycopy( src, 0, dest, destOff, len );
		return destOff + len;
	}

	public static int putOrdinal(byte[] b, int off, Enum val) {
		b[ off ] = (byte) val.ordinal();
		return off + 1;
	}

	public static <X extends Enum> X getEnum(byte[] b, int off, Class<X> clazz) {
		return clazz.getEnumConstants()[ b[ off ] ];
	}

	public static int putFloat(byte[] b, int off, float val) {
		int i = Float.floatToIntBits( val );
		b[ off + 3 ] = (byte) (i >>> 0);
		b[ off + 2 ] = (byte) (i >>> 8);
		b[ off + 1 ] = (byte) (i >>> 16);
		b[ off + 0 ] = (byte) (i >>> 24);
		return off + 4;
	}

	public static int putLong(byte[] b, int off, long val) {
		b[ off + 7 ] = (byte) (val >>> 0);
		b[ off + 6 ] = (byte) (val >>> 8);
		b[ off + 5 ] = (byte) (val >>> 16);
		b[ off + 4 ] = (byte) (val >>> 24);
		b[ off + 3 ] = (byte) (val >>> 32);
		b[ off + 2 ] = (byte) (val >>> 40);
		b[ off + 1 ] = (byte) (val >>> 48);
		b[ off + 0 ] = (byte) (val >>> 56);

		return off + 8;
	}

	public static int putDouble(byte[] b, int off, double val) {
		long j = Double.doubleToLongBits( val );
		b[ off + 7 ] = (byte) (j >>> 0);
		b[ off + 6 ] = (byte) (j >>> 8);
		b[ off + 5 ] = (byte) (j >>> 16);
		b[ off + 4 ] = (byte) (j >>> 24);
		b[ off + 3 ] = (byte) (j >>> 32);
		b[ off + 2 ] = (byte) (j >>> 40);
		b[ off + 1 ] = (byte) (j >>> 48);
		b[ off + 0 ] = (byte) (j >>> 56);
		return off + 8;
	}

	public static int putByte(byte[] rowKey, int idx, int k) {
		rowKey[ idx ] = (byte) k;
		return idx + 1;
	}

	public static int putIntBigEndian(byte[] b, int off, int val) {
		b[ off + 0 ] = (byte) (val >>> 0);
		b[ off + 1 ] = (byte) (val >>> 8);
		b[ off + 2 ] = (byte) (val >>> 16);
		b[ off + 3 ] = (byte) (val >>> 24);
		return off + 4;
	}



    public static class DataBuf {

		public final byte[] bytes;
		public int offset;

		public DataBuf(byte[] bytes) {
			this.bytes = bytes;
		}

        public DataBuf(byte[] bytes, int offset) {
            this.bytes = bytes;
            this.offset = offset;
        }


        public int available() {
            return bytes.length - offset;
        }

		public int readInt() {
			final int value = getInt( bytes, offset );
			offset += 4;
			return value;
		}

		public byte readByte() {
			final byte value = bytes[ offset ];
			offset += 1;
			return value;
		}

		public long readLong() {
			final long aLong = getLong( bytes, offset );
			offset += 8;
			return aLong;
		}

		public <X extends Enum> X readEnum(Class<X> enumClass) {
			final X value = getEnum( bytes, offset, enumClass );
			offset += 1;
			return value;
		}

		public byte[] readBytes(int size) {
			byte[] b = new byte[ size ];

			System.arraycopy( bytes, offset, b, 0, size );
			offset += size;
			return b;
		}

        public byte[] readPrefixedArray() {
            int len = getInt( bytes, offset );
            offset += 4;
            return readBytes(len);
        }

        public byte[] readShortPrefixedArray() {
            int len = getShort( bytes, offset );
            offset += 2;
            return readBytes(len);
        }


		public String readString() {
			String str = null;

			if ( offset == bytes.length + 2 ) {
				return "";
			}

			for ( int i = offset; i < bytes.length - 1; i++ ) {
				if ( bytes[ i ] == 0x00 && bytes[ i + 1 ] == 0x00 ) {
					final int length = i - offset;
					str = new String( bytes, offset, length );
					offset = i + 2;
					break;
				}
			}

			if ( str == null ) {
				throw new IllegalStateException( "No String" );
			}

			return str;
		}

		public short readShort() {
			short res = getShort( bytes, offset );
			offset += 2;
			return res;
		}

		public void finalAssert() {
			if ( offset != bytes.length ) {
				throw new AssertionError( "Have unreaded bytes: " + offset + " / " + bytes.length );
			}
		}
	}

	public static final String UTF8_ENCODING = "UTF-8";

	/*public static byte[] stringBytes(String s) {
		try {
			final byte[] bytes = s.getBytes( UTF8_ENCODING );

			final byte[] out = new byte[ bytes.length + 2 ];
			System.arraycopy( bytes, 0, out, 0, bytes.length );

			return bytes;
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException( e );
		}
	}*/

/*
	public static int stringLengthX0(String str) {
		return str.length() * 2 + 2;
	}
*/

	public static byte[] getString(String string) {

		final byte[] bytes;
		try {
			bytes = string.getBytes( UTF8_ENCODING );
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException( e );
		}
		// two bytes for END prefix
//		final byte[] out = new byte[ bytes.length + 2 ];

		/*	System.arraycopy( bytes, 0, out, off, bytes.length );
				out[ bytes.length ] = 0x00;
				out[ bytes.length + 1 ] = 0x00;

				return off + bytes.length + 2;*/
		return bytes;
	}


//	public String getString()


	public static void main(String[] args) {
		String a = "первая строка";
		String b = "вторая строка";
		String c = "";
		int i = 0;
		int j = 2;

		byte[] b1 = getString( a );
		byte[] b2 = getString( b );
		byte[] b3 = getString( c );

		byte[] bytes = new byte[ 4 + b1.length + 2 + b2.length + 2 + b3.length + 2 + 4 ];

		int idx = 0;
		idx = PrimitiveBits.putInt( bytes, idx, i );
		idx = PrimitiveBits.putStringX0( bytes, idx, b1 );
		idx = PrimitiveBits.putStringX0( bytes, idx, b2 );
		idx = PrimitiveBits.putStringX0( bytes, idx, b3 );
		idx = PrimitiveBits.putInt( bytes, idx, j );

		if ( idx != bytes.length ) {
			throw new AssertionError();
		}
		final DataBuf bb = new DataBuf( bytes );
		if ( i != bb.readInt() ) {
			throw new AssertionError();
		}
		final String anObject = bb.readString();
		if ( !a.equals( anObject ) ) {
			throw new AssertionError( "a = " + anObject );
		}
		if ( !b.equals( bb.readString() ) ) {
			throw new AssertionError();
		}
		if ( !c.equals( bb.readString() ) ) {
			throw new AssertionError();
		}
		if ( j != bb.readInt() ) {
			throw new AssertionError();
		}
	}

/*
    public static int putPrefixedLengthString( byte[] bytes, int idx, String str ) {

    }
*/

	public static int putStringX0(byte[] bytes, int idx, byte[] b3) {
		final int i = putBytes( bytes, idx, b3 );
		bytes[ i ] = 0x00;
		bytes[ 1 + i ] = 0x00;
		return i + 2;
	}


	/**
	 * Write a printable representation of a byte array.
	 *
	 * @param b byte array
	 * @return string
	 * @see #toStringBinary(byte[], int, int)
	 */
	public static String toStringBinary(final byte[] b) {
		return toStringBinary( b, 0, b.length );
	}

    public static String hexString( byte[] b ) {
        return MD5Util.convertToHex(b);
    }

	/**
	 * Write a printable representation of a byte array. Non-printable
	 * characters are hex escaped in the format \\x%02X, eg:
	 * \x00 \x05 etc
	 *
	 * @param b   array to write out
	 * @param off offset to start at
	 * @param len length to write
	 * @return string output
	 */
	public static String toStringBinary(final byte[] b, int off, int len) {
		StringBuilder result = new StringBuilder();
		try {
			String first = new String( b, off, len, "ISO-8859-1" );
			for ( int i = 0; i < first.length(); ++i ) {
				int ch = first.charAt( i ) & 0xFF;
				if ( (ch >= '0' && ch <= '9')
						|| (ch >= 'A' && ch <= 'Z')
						|| (ch >= 'a' && ch <= 'z')
						|| " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf( ch ) >= 0 ) {
					result.append( first.charAt( i ) );
				} else {
					result.append( String.format( "\\x%02X", ch ) );
				}
			}
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		}
		return result.toString();
	}

    public static byte compressByte(int value) {
        if (value > Byte.MAX_VALUE * 2 || value < 0) {
            throw new IllegalArgumentException("Bad value: " + value);
        }
        return (byte) (value - Byte.MAX_VALUE);
    }

    public static int decompressByte(byte value) {
        return value + Byte.MAX_VALUE;
    }

    public static short compressShort(int value) {
        if (value > Short.MAX_VALUE * 2 || value < 0) {
            throw new IllegalArgumentException("Bad value: " + value);
        }
        return (short) (value - Short.MAX_VALUE);
    }

    public static int decompressShort(short value) {
        return value + Short.MAX_VALUE;
    }

    public static int compressInt(long value) {
        if (value > Integer.MAX_VALUE * 2L || value < 0) {
            throw new IllegalArgumentException("Bad value: " + value);
        }
        return (int) (value - Integer.MAX_VALUE);
    }

    public static long decompressInt(int value) {
        return ((long) value) + Integer.MAX_VALUE;
    }



//////////////


    /**
     * Convert a boolean to a byte array. True becomes -1
     * and false becomes 0.
     *
     * @param b value
     * @return <code>b</code> encoded in a byte array.
     */
    public static byte [] toBytes(final boolean b) {
      return new byte[] { b ? (byte) -1 : (byte) 0 };
    }

    /**
     * Reverses {@link #toBytes(boolean)}
     * @param b array
     * @return True or false.
     */
    public static boolean toBoolean(final byte [] b) {
      if (b.length != 1) {
        throw new IllegalArgumentException("Array has wrong size: " + b.length);
      }
      return b[0] != (byte) 0;
    }

    /**
     * Convert a long value to a byte array using big-endian.
     *
     * @param val value to convert
     * @return the byte array
     */
    public static byte[] toBytes(long val) {
      byte [] b = new byte[8];
      for (int i = 7; i > 0; i--) {
        b[i] = (byte) val;
        val >>>= 8;
      }
      b[0] = (byte) val;
      return b;
    }

    public static byte[] toBytes(double val) {
        return toBytes(Double.doubleToLongBits(val));
    }

    /**
     * Converts a byte array to a long value. Reverses
     * {@link #toBytes(long)}
     * @param bytes array
     * @return the long value
     */
    public static long toLong(byte[] bytes) {
      return toLong(bytes, 0, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value. Assumes there will be
     * {@link #SIZEOF_LONG} bytes available.
     *
     * @param bytes bytes
     * @param offset offset
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset) {
      return toLong(bytes, offset, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value.
     *
     * @param bytes array of bytes
     * @param offset offset into array
     * @param length length of data (must be {@link #SIZEOF_LONG})
     * @return the long value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_LONG} or
     * if there's not enough room in the array at the offset indicated.
     */
    public static long toLong(byte[] bytes, int offset, final int length) {
      if (length != SIZEOF_LONG || offset + length > bytes.length) {
        throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
      }
      long l = 0;
      for(int i = offset; i < offset + length; i++) {
        l <<= 8;
        l ^= bytes[i] & 0xFF;
      }
      return l;
    }

    private static IllegalArgumentException
      explainWrongLengthOrOffset(final byte[] bytes,
                                 final int offset,
                                 final int length,
                                 final int expectedLength) {
      String reason;
      if (length != expectedLength) {
        reason = "Wrong length: " + length + ", expected " + expectedLength;
      } else {
       reason = "offset (" + offset + ") + length (" + length + ") exceed the"
          + " capacity of the array: " + bytes.length;
      }
      return new IllegalArgumentException(reason);
    }
    /**
     * @param bytes byte array
     * @return Return double made from passed bytes.
     */
    public static double toDouble(final byte [] bytes) {
      return toDouble(bytes, 0);
    }

    /**
     * @param bytes byte array
     * @param offset offset where double is
     * @return Return double made from passed bytes.
     */
    public static double toDouble(final byte [] bytes, final int offset) {
      return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
    }
    /**
     * Convert an int value to a byte array
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(int val) {
      byte [] b = new byte[4];
      for(int i = 3; i > 0; i--) {
        b[i] = (byte) val;
        val >>>= 8;
      }
      b[0] = (byte) val;
      return b;
    }

    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @return the int value
     */
    public static int toInt(byte[] bytes) {
      return toInt(bytes, 0, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @param offset offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
      return toInt(bytes, offset, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @param offset offset into array
     * @param length length of int (has to be {@link #SIZEOF_INT})
     * @return the int value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
     * if there's not enough room in the array at the offset indicated.
     */
    public static int toInt(byte[] bytes, int offset, final int length) {
      if (length != SIZEOF_INT || offset + length > bytes.length) {
        throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
      }
      int n = 0;
      for(int i = offset; i < (offset + length); i++) {
        n <<= 8;
        n ^= bytes[i] & 0xFF;
      }
      return n;
    }

    /**
     * Convert a short value to a byte array of {@link #SIZEOF_SHORT} bytes long.
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(short val) {
      byte[] b = new byte[SIZEOF_SHORT];
      b[1] = (byte) val;
      val >>= 8;
      b[0] = (byte) val;
      return b;
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @return the short value
     */
    public static short toShort(byte[] bytes) {
      return toShort(bytes, 0, SIZEOF_SHORT);
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @param offset offset into array
     * @return the short value
     */
    public static short toShort(byte[] bytes, int offset) {
      return toShort(bytes, offset, SIZEOF_SHORT);
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @param offset offset into array
     * @param length length, has to be {@link #SIZEOF_SHORT}
     * @return the short value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_SHORT}
     * or if there's not enough room in the array at the offset indicated.
     */
    public static short toShort(byte[] bytes, int offset, final int length) {
      if (length != SIZEOF_SHORT || offset + length > bytes.length) {
        throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
      }
      short n = 0;
      n ^= bytes[offset] & 0xFF;
      n <<= 8;
      n ^= bytes[offset+1] & 0xFF;
      return n;
    }
    /**
     * @param b Presumed UTF-8 encoded byte array.
     * @return String made from <code>b</code>
     */
    public static String toString(final byte [] b) {
      if (b == null) {
        return null;
      }
      return toString(b, 0, b.length);
    }

    /**
     * Joins two byte arrays together using a separator.
     * @param b1 The first byte array.
     * @param sep The separator to use.
     * @param b2 The second byte array.
     */
    public static String toString(final byte [] b1,
                                  String sep,
                                  final byte [] b2) {
      return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
    }

    /**
     * This method will convert utf8 encoded bytes into a string. If
     * an UnsupportedEncodingException occurs, this method will eat it
     * and return null instead.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @param len length of utf-8 sequence
     * @return String made from <code>b</code> or null
     */
    public static String toString(final byte [] b, int off, int len) {
      if (b == null) {
        return null;
      }
      if (len == 0) {
        return "";
      }
      try {
        return new String(b, off, len, UTF8_ENCODING);
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    /**
     * Converts a string to a UTF-8 byte array.
     * @param s string
     * @return the byte array
     */
    public static byte[] toBytes(String s) {
      try {
        return s.getBytes(UTF8_ENCODING);
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }


    /**
     * @param a lower half
     * @param b upper half
     * @return New array that has a in lower half and b in upper half.
     */
    public static byte [] add(final byte [] a, final byte [] b) {
      return add(a, b, EMPTY);
    }

    /**
     * @param a first third
     * @param b second third
     * @param c third third
     * @return New array made from a, b and c
     */
    public static byte [] add(final byte [] a, final byte [] b, final byte [] c) {
      byte [] result = new byte[a.length + b.length + c.length];
      System.arraycopy(a, 0, result, 0, a.length);
      System.arraycopy(b, 0, result, a.length, b.length);
      System.arraycopy(c, 0, result, a.length + b.length, c.length);
      return result;
    }


    public static byte[] intToBytes( int value ) {
        byte[] bytes =  new byte[4];
        putInt(bytes, 0, value);

        return bytes;
    }

    public static byte[] shortToBytes(int value) {
        byte[] bytes =  new byte[2];
        putShort(bytes, 0, (short) value);

        return bytes;

    }


    public static byte[] longToBytes( long value ) {
        byte[] bytes =  new byte[8];
        putLong(bytes, 0, value);

        return bytes;
    }

    public static int findUTFLength(CharSequence str) {
        int strlen = str.length();

        int utflen = 0, c;/* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0000) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        return utflen;
    }

    public static byte[] charSequenceToBytes(CharSequence str) {
        int strlen = str.length();
        int c;
        int i;

        byte[] array = new byte[ findUTFLength(str) ];
        int pos = 0;

        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0000) && (c <= 0x007F)))
                break;

            array[pos++] = (byte) c;
            //write(c);
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0000) && (c <= 0x007F)) {
                array[pos++] = (byte) c;

            } else if (c > 0x07FF) {
                array[pos++] =((byte) (0xE0 | ((c >> 12) & 0x0F)));
                array[pos++] =((byte) (0x80 | ((c >> 6) & 0x3F)));
                array[pos++] =((byte) (0x80 | (c & 0x3F)));
            } else {
                array[pos++] =((byte) (0xC0 | ((c >> 6) & 0x1F)));
                array[pos++] =((byte) (0x80 | c & 0x3F));
            }
        }

        return array;
    }


    public static byte[] twoLongs( long l1, long l2) {
        final byte[] bytes = new byte[16];
        putLong( bytes, 0, l1);
        putLong( bytes, 8, l2);
        return bytes;
    }

    public static byte[] twoInts( int l1, int l2) {
        final byte[] bytes = new byte[8];
        putInt( bytes, 0, l1);
        putInt( bytes, 4, l2);
        return bytes;
    }

}
