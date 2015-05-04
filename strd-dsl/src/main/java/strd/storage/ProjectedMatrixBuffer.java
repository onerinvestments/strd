package strd.storage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import gnu.trove.procedure.TObjectObjectProcedure;
import strd.node.impl.databuffers.DataBufferVisitor;

/**
 * User: light
 * Date: 01/12/13
 * Time: 03:19
 */
public class ProjectedMatrixBuffer implements MatrixBuffer, KryoSerializable, StrdNodeResult {

    private final BytesMatrixTHashMap buffer = new BytesMatrixTHashMap();

    public void clear() {
        buffer.clear();
    }

    public void acceptVisitor(final ProjectedMatrixBufferVisitor visitor) {
        buffer.forEachEntry(new TObjectObjectProcedure<byte[], FieldFunctionStates>() {
            @Override
            public boolean execute(byte[] a, FieldFunctionStates b) {
                visitor.visit(a, b);
                return true;
            }
        });
    }

    public void filter(final FieldDeclaration[] fields) {

        buffer.retainEntries(new TObjectObjectProcedure<byte[], FieldFunctionStates>() {
            @Override
            public boolean execute(byte[] a, FieldFunctionStates b) {
                boolean alive = true;
                for (FieldDeclaration field : fields) {
                    int n = field.matrixOutputNum;
                    if (field.fn.storeSkip(b.get(n))) {
                        alive = false;
                        break;
                    }
                }
                return alive;
            }
        });
    }

    @Override
    public void append(StrdNodeResult buf2, final FieldDeclaration[] functions) {
        append0((ProjectedMatrixBuffer) buf2, functions);
    }

    public void append0(ProjectedMatrixBuffer buf2, final FieldDeclaration[] functions) {
        final BytesMatrixTHashMap m2 = buf2.buffer;

        m2.forEachEntry(new TObjectObjectProcedure<byte[], FieldFunctionStates>() {
            @SuppressWarnings("unchecked")
            @Override
            public boolean execute(byte[] a, FieldFunctionStates b) {
                FieldFunctionStates record1 = buffer.get(a);

                if (record1 == null) {
                    buffer.put(a, b);
                } else {
                    record1.mergeWith(b, functions);
                }

                return true;
            }
        });

    }

    /*
    @Override
    public MatrixBuffer merge(MatrixBuffer _buf2, final FieldDeclaration[] functions) {
        ProjectedMatrixBuffer buf2 = (ProjectedMatrixBuffer) _buf2;

        final THashMap<byte[], FieldFunctionState[]> m1;
        final THashMap<byte[], FieldFunctionState[]> m2;

        if (buffer.size() > buf2.buffer.size()) {
            m1 = this.buffer;
            m2 = buf2.buffer;
        } else {
            m1 = buf2.buffer;
            m2 = this.buffer;
        }

        m2.forEachEntry( new TObjectObjectProcedure<byte[], FieldFunctionState[]>() {
            @SuppressWarnings("unchecked")
            @Override
            public boolean execute(byte[] a, FieldFunctionState[] b) {

                FieldFunctionState[] record1 = m1.get(a);

                if (record1 == null) {
                    m1.put( a, b );
                } else {

                    for (FieldDeclaration function : functions) {
                        int n = function.matrixOutputNum;
                        function.fn.merge(record1[n], b[n], record1[n]);
                    }

                }

                return true;
            }
        });

        return buffer.size() > buf2.buffer.size() ? this : buf2;
    }
*/

    public int size() {
        return buffer.size();
    }

    @Override
    public void acceptVisitor(MatrixBufferVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acceptVisitor(DataBufferVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    public FieldFunctionStates getRow(byte[] projKey) {
        return buffer.get(projKey);
    }

    public void putRow(byte[] projKey, FieldFunctionStates currentRecord) {
        buffer.put(projKey, currentRecord);
    }

    @Override
    public String toString() {
        return "ProjectedMatrixBuffer{" +
                "buffer=" + buffer.size() +
                '}';
    }

    @Override
    public void write(Kryo kryo, final Output output) {
        output.writeInt(buffer.size());
        if (! buffer.isEmpty()) {

            buffer.forEachEntry( new TObjectObjectProcedure<byte[], FieldFunctionStates>() {
                boolean headerWritten = false;

                @Override
                public boolean execute(byte[] a, FieldFunctionStates b) {
                    final int ffCount = b.getCount();

                    if (!headerWritten) {

                        output.writeByte(ffCount);
                        for (int i = 0; i< ffCount; i++) {
                            output.writeByte(b.get(i).serialId());
                        }
                        headerWritten = true;
                    }
                    // write key
                    output.writeShort( a.length );
                    output.write(a);
                    // write value
                    for (int i = 0; i < ffCount; i++) {
                        b.get(i).write(output);
                    }
                    return true;
                }
            });
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        buffer.clear();


        int length = input.readInt(); // length total
        if (length > 0) {
            byte ffsCount = input.readByte(); // ffstates.length
            byte[] ffTypes = new byte[ffsCount];

            for (int f = 0; f < ffsCount; f++) {
                ffTypes[f] = input.readByte(); // each ffstate
            }

            for (int i = 0; i < length; i++) {
                short keyLen = input.readShort(); // key len
                byte[] key = new byte[keyLen];

                if (input.read(key) != keyLen) {
                    throw new IllegalStateException();
                }

                FieldFunctionStates states = new FieldFunctionStates(ffsCount);

                for (int f = 0; f < ffsCount; f++) {
                    final FieldFunctionState state = FieldFunctionStates.forType(ffTypes[f]);
                    state.read(input); // ffstate
                    states.set(f, state);
                }

                buffer.put(key, states);
            }
        }
    }
 }
