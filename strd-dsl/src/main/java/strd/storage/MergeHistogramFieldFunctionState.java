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
public class MergeHistogramFieldFunctionState implements FieldFunctionState {

    public static final byte ID = 10;

    public Histogram state;


    @Override
    public void write(OutputStream os) {
        try {
            os.write(PrimitiveBits.longToBytes(state.count()));
            os.write(PrimitiveBits.longToBytes(state.sum()));
            os.write(PrimitiveBits.intToBytes(state.buckets().length));
            for (long l : state.buckets()) {
                os.write(PrimitiveBits.longToBytes(l));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void read(PrimitiveBits.DataBuf buf) {
        state = new Histogram();
        state.count_$eq(buf.readLong());
        state.sum_$eq(buf.readLong());
        int len = buf.readInt();
        if (state.buckets().length != len) {
            throw new IllegalStateException("Histogram buckets length incompatible");
        }

        for (int i = 0; i < len; i++) {
            state.buckets()[i] = buf.readLong();
        }
    }

    @Override
    public byte serialId() {
        return ID;
    }

    @Override
    public int estimatedSize() {
        return 8 + 8 + 4 + state.buckets().length * 8;
    }

    @Override
    public void read(Input input) {
        state = new Histogram();
        state.count_$eq(input.readLong());
        state.sum_$eq(input.readLong());
        int len = input.readInt();
        if (state.buckets().length != len) {
            throw new IllegalStateException("Histogram buckets length incompatible");
        }

        for (int i = 0; i < len; i++) {
            state.buckets()[i] = input.readLong();
        }

    }
}
