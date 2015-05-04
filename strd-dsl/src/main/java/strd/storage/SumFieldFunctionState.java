package strd.storage;

import com.esotericsoftware.kryo.io.Input;
import com.twitter.ostrich.stats.Histogram;
import lmbrd.zn.util.PrimitiveBits;

import java.io.IOException;
import java.io.OutputStream;

/**
 * User: penkov
 * Date: 26.03.14
 * Time: 17:47
 */
public class SumFieldFunctionState implements FieldFunctionState {

    public static final byte ID = 11;

    public long state = 0;


    @Override
    public void write(OutputStream os) {
        try {
            os.write(PrimitiveBits.longToBytes(state));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void read(PrimitiveBits.DataBuf buf) {
        state = buf.readLong();
    }

    @Override
    public byte serialId() {
        return ID;
    }

    @Override
    public int estimatedSize() {
        return 8;
    }

    @Override
    public void read(Input input) {
        state = input.readLong();
    }
}
