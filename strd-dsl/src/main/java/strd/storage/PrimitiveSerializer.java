package strd.storage;

import com.esotericsoftware.kryo.io.Input;
import lmbrd.zn.util.PrimitiveBits;

import java.io.IOException;
import java.io.OutputStream;

/**
 * User: light
 * Date: 01/12/13
 * Time: 08:36
 */
public class PrimitiveSerializer {


    public static void write(Object obj, OutputStream os) {
        byte type = getType(obj);
        try {
            os.write(type);
            write(type, obj, os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void write(int type, Object obj, OutputStream os) {
        try {
            if (type == 1) {
                os.write(PrimitiveBits.intToBytes((Integer) obj));
            } else if (type == 2) {
                os.write(PrimitiveBits.longToBytes((Long) obj));
            } else if (type == 3) {
                String s = (String) obj;
                byte[] bytes = s.getBytes(PrimitiveBits.UTF8_ENCODING);
                os.write(PrimitiveBits.shortToBytes(bytes.length));
                os.write(bytes);
            } else {
                throw new IllegalArgumentException("bad type : " + obj.getClass());
            }
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    public static byte getType(Object obj) {
        if (obj instanceof Integer) {
            return 1;
        } else if (obj instanceof Long) {
            return 2;
        } else if (obj instanceof String) {
            return 3;
        } else {
            throw new IllegalArgumentException("bad type : " + obj.getClass());
        }
    }

    public static Object read(PrimitiveBits.DataBuf buf) {
        byte type = buf.readByte();

        return readForType(type, buf);
    }

    public static Object read(Input buf) {
        byte type = buf.readByte();
        return readForType(type, buf);
    }

    public static Object readForType(byte type, PrimitiveBits.DataBuf buf) {
        if (type == 1) {
            return buf.readInt();
        } else if (type == 2) {
            return buf.readLong();
        } else if (type == 3) {
            byte[] bytes = buf.readBytes( buf.readShort() );

            return new String(bytes);
        } else {
            throw new IllegalArgumentException("bad type : " + type);
        }
    }

    public static Object readForType(byte type, Input buf) {
        if (type == 1) {
            return buf.readInt();
        } else if (type == 2) {
            return buf.readLong();
        } else if (type == 3) {
            byte[] bytes = buf.readBytes( buf.readShort() );
            return new String(bytes);
        } else {
            throw new IllegalArgumentException("bad type : " + type);
        }
    }

    public static int estimatedSize(Object obj) {
        if (obj instanceof Integer) {
            return 4;
        } else if (obj instanceof Long) {
            return 8;
        } else if (obj instanceof String) {
            return 2 + PrimitiveBits.findUTFLength((String) obj);
        } else {
            throw new IllegalArgumentException("bad type : " + obj.getClass());
        }

    }

}
