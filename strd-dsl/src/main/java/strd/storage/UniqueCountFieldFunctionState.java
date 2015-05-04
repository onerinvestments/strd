package strd.storage;

import com.esotericsoftware.kryo.io.Input;
import gnu.trove.procedure.TObjectProcedure;
import gnu.trove.set.hash.THashSet;
import lmbrd.zn.HyperLogLog;
import lmbrd.zn.util.PrimitiveBits;
import lmbrd.zn.util.UUID;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 9/9/13
 * Time: 12:52 PM
 */
@SuppressWarnings("unchecked")
public class UniqueCountFieldFunctionState implements FieldFunctionState {
    public static final byte ID = 2;

    public static HyperLogLog.Builder counterBuilder = new HyperLogLog.Builder(0.005);

    public THashSet set;
    public HyperLogLog state;


    @Override
    public void write(final OutputStream os) {


        try {

            if (state != null) {
                os.write(1);

                state.write(os);
            } else if (set != null) {
                os.write(2);

                os.write(PrimitiveBits.intToBytes(set.size()));

                if (!set.isEmpty()) {
                    set.forEach(new TObjectProcedure() {
                        int type = -1;

                        @Override
                        public boolean execute(Object object) {
                            try {
                                if (type == -1) {
                                    type = PrimitiveSerializer.getType(object);
                                    os.write(type);
                                }

                                PrimitiveSerializer.write(type, object, os);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            return true;
                        }
                    });
                }

            } else {
                os.write(3);
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public long size() {
        if (state != null) {
            return state.cardinality();
        } else if (set != null) {
            return set.size();
        } else {
            return 0;
        }
    }

    public void add(Object obj) {
        if (set == null) {
            set = new THashSet();
        }
        set.add(obj);
    }

    public void addToHyperLogLog(Object obj) {
        if (state == null) {
            state = counterBuilder.build();
        }

        if (obj instanceof String) {
            state.offer(((String) obj).getBytes());
        } else if (obj instanceof Integer) {
            state.offer(PrimitiveBits.intToBytes((Integer) obj));
        } else if (obj instanceof Long) {
            state.offer(PrimitiveBits.longToBytes((Long) obj));
        }
    }

//    private final BloomFilter bloomFilter = new BloomFilter();

/*

    public THashSet set = new THashSet<>();

    @Override
    public void write(final OutputStream os) {
        int size = set.size();
        try {
            os.write(PrimitiveBits.intToBytes(size));

            if (size > 0) {
                set.forEach(new TObjectProcedure() {
                    int type = -1;

                    @Override
                    public boolean execute(Object object) {
                        try {
                            if (type == -1) {
                                type = PrimitiveSerializer.getType(object);
                                os.write(type);
                            }

                            PrimitiveSerializer.write(type, object, os);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return true;
                    }
                });
            }
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

*/

    @Override
    public void read(PrimitiveBits.DataBuf buf) {
        set = null;
        state = null;

        byte typeState = buf.readByte();
        if (typeState == 1) {
            state = HyperLogLog.read(buf);
        } else if (typeState == 2) {
            int size = buf.readInt();
            set = new THashSet(size);

            if (size > 0) {
                byte type = buf.readByte();

                for (int i = 0; i < size; i++) {
                    set.add(PrimitiveSerializer.readForType(type, buf));
                }
            }
        } else if (typeState == 3) {
            set = null;
            state = null;
            // EMPTY
        } else {
            throw new IllegalArgumentException("bad state=" + typeState);
        }
    }
    @Override
    public void read(Input buf) {
        set = null;
        state = null;

        byte typeState = buf.readByte();
        if (typeState == 1) {
            state = HyperLogLog.read(buf);
        } else if (typeState == 2) {
            int size = buf.readInt();
            set = new THashSet(size);

            if (size > 0) {
                byte type = buf.readByte();

                for (int i = 0; i < size; i++) {
                    set.add(PrimitiveSerializer.readForType(type, buf));
                }
            }
        } else if (typeState == 3) {
            set = null;
            state = null;
            // EMPTY
        } else {
            throw new IllegalArgumentException("bad state=" + typeState);
        }

    }

    @Override
    public byte serialId() {
        return ID;
    }

    @Override
    public int estimatedSize() {
        if (state != null) {
            return state.sizeInBytes() + 1;
        } else {
            if (set.isEmpty()) {
                return 1;
            } else {
                return 1 + 4 + PrimitiveSerializer.estimatedSize(set.iterator().next());
            }
        }
    }


    public static void main(String[] args) {
        final UniqueCountFieldFunctionState s = new UniqueCountFieldFunctionState();
        for (int i =0; i < 10000; i++) {
            s.addToHyperLogLog(new UUID().toString());
        }

        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        s.write(bo);
        System.out.println( " size:"+bo.toByteArray().length);
    }
}
