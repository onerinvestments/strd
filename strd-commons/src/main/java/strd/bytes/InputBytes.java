package strd.bytes;

import java.io.IOException;

/**
 * User: light
 * Date: 7/16/13
 * Time: 6:24 PM
 */
public interface InputBytes {

    long readLong();

    byte[] readBytes(int size);

    void readBytes(byte[] bytes);
    void readBytes(byte[] bytes, int off, int len);

    int readIntLittleEndianPaddedOnBitWidth(int bitWidth) throws IOException;

    int readUnsignedVarInt();

    int read();

    void skipBytes(int count);

    int available();

    int position();

    void close();

}
