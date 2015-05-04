package strd.bytes;

import lmbrd.io.JVMReservedMemory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * User: light
 * Date: 7/16/13
 * Time: 6:30 PM
 */
public class BytesChannelWrapper implements InputBytes {

    private ByteBuffer buffer;

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public BytesChannelWrapper(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public BytesChannelWrapper(byte[] byteArray) {
        this.buffer = ByteBuffer.wrap( byteArray );
    }

    @Override
    public long readLong() {
        return buffer.getLong();
    }

    @Override
    public byte[] readBytes(int size) {

        byte[] bytes = new byte[size];
        buffer.get(bytes);
        buffer.flip();
        return bytes;
    }

    @Override
    public int readUnsignedVarInt() {


        int value = 0;
        int i = 0;
        int b;

        int iters = 0;

        while (((b = read()) & 0x80) != 0) {
            // отсечка от бесконечного цикла на не правильном формате данных
            if (iters ++ > 128) throw new RuntimeException("Can not read varint");

            value |= (b & 0x7F) << i;
            i += 7;
        }

        return value | (b << i);
    }

    @Override
    public void skipBytes(int count) {
        buffer.position( buffer.position() + count );
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public int position() {
        return buffer.position();
    }

    @Override
    public void close() {
        JVMReservedMemory.cleanBuffer( buffer );
        this.buffer = null;
    }

    @Override
    public void readBytes(byte[] bytes) {
        buffer.get(bytes);
    }

    @Override
    public void readBytes(byte[] bytes, int off, int len) {
        buffer.get( bytes, off, len);
    }

    /**
     * @param bitLength a count of bits
     * @return the corresponding byte count padded to the next byte
     */
    public static int paddedByteCountFromBits(int bitLength) {
        return (bitLength + 7) / 8;
    }

    @Override
    public int readIntLittleEndianPaddedOnBitWidth(int bitWidth) throws IOException {
        int bytesWidth = paddedByteCountFromBits(bitWidth);
            switch (bytesWidth) {
                case 0:
                    return 0;
                case 1:
                    return readIntLittleEndianOnOneByte();
                case 2:
                    return readIntLittleEndianOnTwoBytes();
                case 3:
                    return readIntLittleEndianOnThreeBytes();
                case 4:
                    return readIntLittleEndian();
                default:
                    throw new IOException(
                            String.format("Encountered bitWidth (%d) that requires more than 4 bytes", bitWidth));
            }
    }


    /**
     * reads an int in little endian at the given position
     *
     * @param in
     * @param offset
     * @return
     * @throws IOException
     */
    public  int readIntLittleEndian(byte[] in, int offset) throws IOException {
        int ch4 = in[offset] & 0xff;
        int ch3 = in[offset + 1] & 0xff;
        int ch2 = in[offset + 2] & 0xff;
        int ch1 = in[offset + 3] & 0xff;
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public int readIntLittleEndian() throws IOException {
        // TODO: this is duplicated code in LittleEndianDataInputStream
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
    }

    public  int readIntLittleEndianOnOneByte() throws IOException {
        int ch1 = read();
        if (ch1 < 0) {
            throw new EOFException();
        }
        return ch1;
    }

    public  int readIntLittleEndianOnTwoBytes() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }
        return ((ch2 << 8) + (ch1 << 0));
    }

    public int read()  {
        return buffer.get() & 0xFF;
    }

    public  int readIntLittleEndianOnThreeBytes() throws IOException {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        if ((ch1 | ch2 | ch3) < 0) {
            throw new EOFException();
        }
        return ((ch3 << 16) + (ch2 << 8) + (ch1 << 0));
    }







    public  void writeIntLittleEndianOnOneByte( int v) throws IOException {
        buffer.put((byte) ((v >>> 0) & 0xFF));
    }

    public  void writeIntLittleEndianOnTwoBytes( int v) throws IOException {
        buffer.put((byte) ((v >>> 0) & 0xFF));
        buffer.put((byte) ((v >>> 8) & 0xFF));
    }

    public  void writeIntLittleEndianOnThreeBytes( int v) throws IOException {
        buffer.put((byte) ((v >>> 0) & 0xFF));
        buffer.put((byte) ((v >>> 8) & 0xFF));
        buffer.put((byte) ((v >>> 16) & 0xFF));
    }

    public  void writeIntLittleEndian( int v) throws IOException {
        // TODO: this is duplicated code in LittleEndianDataOutputStream
        buffer.put((byte) ((v >>> 0) & 0xFF));
        buffer.put((byte) ((v >>> 8) & 0xFF));
        buffer.put((byte) ((v >>> 16) & 0xFF));
        buffer.put((byte) ((v >>> 24) & 0xFF));
    }

    /**
     * Write a little endian int to out, using the the number of bytes required by
     * bit width
     */
    public  void writeIntLittleEndianPaddedOnBitWidth( int v, int bitWidth)
            throws IOException {

        int bytesWidth = paddedByteCountFromBits(bitWidth);
        switch (bytesWidth) {
            case 0:
                break;
            case 1:
                writeIntLittleEndianOnOneByte( v);
                break;
            case 2:
                writeIntLittleEndianOnTwoBytes( v);
                break;
            case 3:
                writeIntLittleEndianOnThreeBytes( v);
                break;
            case 4:
                writeIntLittleEndian( v);
                break;
            default:
                throw new IOException(
                        String.format("Encountered value (%d) that requires more than 4 bytes", v));
        }
    }


}
