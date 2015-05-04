package strd.storage;

import com.esotericsoftware.kryo.io.Input;
import lmbrd.zn.util.PrimitiveBits;

import java.io.IOException;
import java.io.OutputStream;

/**
 * User: light
 * Date: 9/2/13
 * Time: 8:15 PM
 */
public class CountFieldFunctionState implements FieldFunctionState {
    public static final byte ID = 1;

    public long count = 0;


    @Override
    public String toString() {
        return "Count{" +  count + "}";
    }

    @Override
    public void write(OutputStream os) {

        try {
            os.write(PrimitiveBits.longToBytes(count));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void read(PrimitiveBits.DataBuf buf) {
        this.count = buf.readLong();
    }

    @Override
    public void read(Input input) {
        this.count = input.readLong();
    }

    @Override
    public byte serialId() {
        return ID;
    }

    @Override
    public int estimatedSize() {
        return 8;
    }

}
