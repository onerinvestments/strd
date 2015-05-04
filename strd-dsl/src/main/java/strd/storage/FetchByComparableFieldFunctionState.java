package strd.storage;

import com.esotericsoftware.kryo.io.Input;
import lmbrd.zn.util.PrimitiveBits;

import java.io.OutputStream;

/**
* User: light
* Date: 9/2/13
* Time: 4:36 PM
*/
public class FetchByComparableFieldFunctionState implements FieldFunctionState {
    public static final byte ID = 3;

    public Object value;
    public Comparable order;

    @Override
    public void read(PrimitiveBits.DataBuf buf) {
        value = PrimitiveSerializer.read(buf);
        order = (Comparable) PrimitiveSerializer.read(buf);
    }
    @Override
    public void read(Input buf) {
        value = PrimitiveSerializer.read(buf);
        order = (Comparable) PrimitiveSerializer.read(buf);
    }

    @Override
    public byte serialId() {
        return ID;
    }

    @Override
    public int estimatedSize() {
        return PrimitiveSerializer.estimatedSize(value) + PrimitiveSerializer.estimatedSize(order);
    }


    @Override
    public void write(OutputStream os) {
        PrimitiveSerializer.write(value, os);
        PrimitiveSerializer.write(order, os);
    }
}
