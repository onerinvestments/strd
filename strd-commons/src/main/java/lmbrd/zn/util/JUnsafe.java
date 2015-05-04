package lmbrd.zn.util;

import org.jetbrains.annotations.NotNull;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

/**
 * User: light
 * Date: 7/29/13
 * Time: 2:20 PM
 */
public class JUnsafe {

    public static final int UNSIGNED_BYTE_MASK = 0xFF;

    public static final Unsafe unsafe;

    static {
        try {
            final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
                public Unsafe run() throws Exception {
                    Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                    theUnsafe.setAccessible(true);
                    return (Unsafe) theUnsafe.get(null);
                }
            };

            unsafe = AccessController.doPrivileged(action);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load unsafe", e);
        }
    }

    // These numbers represent the point at which we have empirically
    // determined that the average cost of a JNI call exceeds the expense
    // of an element by element copy.  These numbers may change over time.
    static final int JNI_COPY_TO_ARRAY_THRESHOLD = 6;
    static final int JNI_COPY_FROM_ARRAY_THRESHOLD = 6;

    // This number limits the number of bytes to copy per call to Unsafe's
    // copyMemory method. A limit is imposed to allow for safepoint polling
    // during a large copy
    static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    /**
     * Copy from given source array to destination address.
     *
     * @param src           source array
     * @param srcBaseOffset offset of first element of storage in source array
     * @param srcPos        offset within source array of the first element to read
     * @param dstAddr       destination address
     * @param length        number of bytes to copy
     */
    public static void copyFromArray(Object src, long srcBaseOffset, long srcPos,
                                     long dstAddr, long length) {
        long offset = srcBaseOffset + srcPos;
        while (length > 0) {
            long size = (length > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : length;
            unsafe.copyMemory(src, offset, null, dstAddr, size);
            length -= size;
            offset += size;
            dstAddr += size;
        }
    }

    public static final long arrayBaseOffset = (long) unsafe.arrayBaseOffset(byte[].class);

    /**
     * Copy from source address into given destination array.
     *
     * @param srcAddr       source address
     * @param dst           destination array
     * @param dstBaseOffset offset of first element of storage in destination array
     * @param dstPos        offset within destination array of the first element to write
     * @param length        number of bytes to copy
     */
    public static void copyToArray(long srcAddr, Object dst, long dstBaseOffset, long dstPos,
                                   long length) {
        long offset = dstBaseOffset + dstPos;
        while (length > 0) {
            long size = (length > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : length;
            unsafe.copyMemory(null, srcAddr, dst, offset, size);
            length -= size;
            srcAddr += size;
            offset += size;
        }
    }

    public static boolean compareUTF0( long offset, @NotNull CharSequence str, long strlen ) {
        int c;
        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0000) && (c <= 0x007F)))
                break;

            if ( unsafe.getByte(offset++) != (byte) c) {
                return false;
            }
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0000) && (c <= 0x007F)) {
                if ( unsafe.getByte(offset++) !=  (byte) c) {
                    return false;
                }
            } else if (c > 0x07FF) {

                if ( unsafe.getByte(offset++) != (byte) (0xE0 | ((c >> 12) & 0x0F)))
                    return false;

                if ( unsafe.getByte(offset++) != (byte) (0x80 | ((c >> 6) & 0x3F)))
                    return false;

                if ( unsafe.getByte(offset++) != (byte) (0x80 | (c & 0x3F)))
                    return false;

            } else {

                if ( unsafe.getByte(offset++) != (byte) (0xC0 | ((c >> 6) & 0x1F)))
                    return false;

                if ( unsafe.getByte(offset++) != (byte) (0x80 | c & 0x3F))
                    return false;
            }
        }
        return true;
    }

    public static void writeUTF0(long offset, @NotNull CharSequence str, long strlen) {
        int c;
        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0000) && (c <= 0x007F)))
                break;
            unsafe.putByte(offset++, (byte) c);
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0000) && (c <= 0x007F)) {
                unsafe.putByte(offset++, (byte) c);
            } else if (c > 0x07FF) {
                unsafe.putByte(offset++, (byte) (0xE0 | ((c >> 12) & 0x0F)));
                unsafe.putByte(offset++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                unsafe.putByte(offset++, (byte) (0x80 | (c & 0x3F)));
            } else {
                unsafe.putByte(offset++, (byte) (0xC0 | ((c >> 6) & 0x1F)));
                unsafe.putByte(offset++, (byte) (0x80 | c & 0x3F));
            }
        }
    }

    public static void readUTF0(long offset, @NotNull Appendable appendable, int bytesLen) throws IOException {

        int count = 0;
        while (count < bytesLen) {
            int c = unsafe.getByte(offset++);

            if (c < 0) {
                offset -=1;
                break;
            }

            count++;
            appendable.append((char) c);
        }

        while (count < bytesLen) {
            int c = unsafe.getByte( offset++ ) & UNSIGNED_BYTE_MASK;

            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                /* 0xxxxxxx */
                    count++;
                    appendable.append((char) c);
                    break;
                case 12:
                case 13: {
                /* 110x xxxx 10xx xxxx */
                    count += 2;
                    if (count > bytesLen)
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    int char2 = unsafe.getByte(offset++) & UNSIGNED_BYTE_MASK;


                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException(
                                "malformed input around byte " + count);
                    int c2 = (char) (((c & 0x1F) << 6) |
                            (char2 & 0x3F));
                    appendable.append((char) c2);
                    break;
                }
                case 14: {
                /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    count += 3;
                    if (count > bytesLen)
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");

                    int char2 = unsafe.getByte(offset++) & UNSIGNED_BYTE_MASK;

                    int char3 = unsafe.getByte(offset++) & UNSIGNED_BYTE_MASK;

                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count - 1));
                    int c3 = (char) (((c & 0x0F) << 12) |
                            ((char2 & 0x3F) << 6) |
                            (char3 & 0x3F));

                    appendable.append((char) c3);
                    break;
                }
                default:
                /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException(
                            "malformed input around byte " + count);
            }
        }
    }

    public static int findUTFBytesLength(@NotNull CharSequence str, long strlen) {
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

}
