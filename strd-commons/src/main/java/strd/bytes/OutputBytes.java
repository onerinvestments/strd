package strd.bytes;

/**
 * User: light
 * Date: 7/16/13
 * Time: 6:26 PM
 */
public interface OutputBytes {

    void writeLong(long value);

    void writeBytes(byte[] bytes);

    void writeVarIntUnsigned(int values);

}
