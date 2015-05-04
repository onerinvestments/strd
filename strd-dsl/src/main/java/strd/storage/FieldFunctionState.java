package strd.storage;

import com.esotericsoftware.kryo.io.Input;
import lmbrd.zn.util.PrimitiveBits;

import java.io.OutputStream;

/**
 * User: light
 * Date: 01/12/13
 * Time: 08:29
 */
public interface FieldFunctionState {
    void write( OutputStream os );
    void read( PrimitiveBits.DataBuf buf );
    byte serialId();

    int estimatedSize();

    void read(Input input);
}
