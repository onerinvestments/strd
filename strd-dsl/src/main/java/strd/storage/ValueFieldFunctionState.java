package strd.storage;

import com.esotericsoftware.kryo.io.Input;
import lmbrd.zn.util.PrimitiveBits;

import java.io.OutputStream;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:54 PM
 */
public class ValueFieldFunctionState implements FieldFunctionState{
    public static final byte ID = 4;

    public Object value;

    @Override
    public String toString() {
        return "Value{" +  value + "}";
    }

    @Override
    public void write(OutputStream os) {
        PrimitiveSerializer.write(value, os);
    }

    @Override
    public void read(PrimitiveBits.DataBuf buf) {
        value = PrimitiveSerializer.read(buf);
    }

    @Override
    public void read(Input buf) {
        value = PrimitiveSerializer.read(buf);
    }

    @Override
    public byte serialId() {
        return ID;
    }

    @Override
    public int estimatedSize() {
        return PrimitiveSerializer.estimatedSize(value);
    }

}
