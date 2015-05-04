package strd.storage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.ostrich.stats.Stats;
import gnu.trove.map.hash.THashMap;
import gnu.trove.procedure.TObjectObjectProcedure;
import gnu.trove.procedure.TObjectProcedure;
import strd.node.impl.databuffers.DataBufferVisitor;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:16 PM
 */
public class HashMatrixBuffer implements MatrixBuffer, KryoSerializable, StrdNodeResult {

    public THashMap<ColumnsJoined, FieldFunctionState[]> list = new THashMap<>();

    public static final String WRITE_METRIC = "kryo/write/HashMatrixBuffer";
    public static final String READ_METRIC = "kryo/read/HashMatrixBuffer";

    @Override
    public void acceptVisitor(DataBufferVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    public void acceptVisitor(final MatrixKeyValueVisitor visitor) {
        list.forEachEntry(new TObjectObjectProcedure<ColumnsJoined, FieldFunctionState[]>() {
            @Override
            public boolean execute(ColumnsJoined a, FieldFunctionState[] b) {
                visitor.visit(a, b);
                return true;
            }
        });
    }

    @Override
    public void acceptVisitor(final MatrixBufferVisitor visitor) {
        if (list.isEmpty()) {
            visitor.visitEmpty();
            return;
        }

        list.forEachValue(new TObjectProcedure<FieldFunctionState[]>() {
            @Override
            public boolean execute(FieldFunctionState[] object) {
                visitor.visit(object);
                return true;
            }
        });
    }

    @Override
    public void append(final StrdNodeResult buf2, final FieldDeclaration[] functions) {
        final THashMap<ColumnsJoined, FieldFunctionState[]> otherList = ((HashMatrixBuffer) buf2).list;

        otherList.forEachEntry(
                new TObjectObjectProcedure<ColumnsJoined, FieldFunctionState[]>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean execute(ColumnsJoined a, FieldFunctionState[] b) {

                        FieldFunctionState[] record1 = list.get(a);

                        if (record1 == null) {
                            list.put(a, b);
                        } else {
                            for (FieldDeclaration function : functions) {
                                int n = function.matrixOutputNum;
                                function.fn.merge(record1[n], b[n], record1[n]);
                            }
                        }

                        return true;
                    }
                }
        );
    }


    public String mkString() {
        final StringBuilder sb = new StringBuilder();
        list.forEachEntry(new TObjectObjectProcedure<ColumnsJoined, Object[]>() {
            @Override
            public boolean execute(ColumnsJoined a, Object[] b) {
                for (Object column : a.columns) {
                    sb.append(column).append(" ");
                }
                sb.append("=> ");
                for (Object o : b) {
                    sb.append(o).append(" ");
                }
                sb.append("\n");
                return true;
            }
        });
        return sb.toString();
    }

    @Override
    public String toString() {
        return "HashMatrixBuffer{" + "list=" + list.size() + '}';
    }

    @Override
    public void write(Kryo kryo, final Output output) {
        long start = System.currentTimeMillis();

        output.writeInt(list.size());
        if (!list.isEmpty()) {

            list.forEachEntry(new TObjectObjectProcedure<ColumnsJoined, FieldFunctionState[]>() {
                boolean headerWritten = false;
                byte types[];

                @Override
                public boolean execute(ColumnsJoined a, FieldFunctionState[] b) {
                    Object[] columns = a.columns;
                    if (!headerWritten) {

                        output.writeShort(a.columns.length);


                        types = new byte[columns.length];

                        for (int i = 0; i < columns.length; i++) {
                            Object column = columns[i];
                            byte type = PrimitiveSerializer.getType(column);
                            types[i] = type;

                            output.write(type);
                        }

                        output.writeShort(b.length);

                        for (FieldFunctionState state : b) {
                            output.write(state.serialId());
                        }

                        headerWritten = true;
                    }

                    for (int i = 0, columnsLength = columns.length; i < columnsLength; i++) {
                        Object column = columns[i];
                        PrimitiveSerializer.write(types[i], column, output);
                    }

                    for (FieldFunctionState state : b) {
                        state.write(output);
                    }

                    return true;
                }
            });
        }

        Stats.addMetric(WRITE_METRIC, (int) (System.currentTimeMillis() - start));
    }

    @Override
    public void read(Kryo kryo, Input input) {
        long start = System.currentTimeMillis();

        list.clear();
        int length = input.readInt();
        if (length == 0) {
            return;
        }

        int colLen = input.readShort();
        byte[] types = new byte[colLen];

        for (int i = 0; i < colLen; i++) {
            types[i] = input.readByte();
        }

        int ffsLen = input.readShort();

        byte[] ffTypes = new byte[ffsLen];
        for (int i = 0; i < ffsLen; i++) {
            ffTypes[i] = input.readByte();
        }

        for (int i = 0; i < length; i++) {
            // read key
            ColumnsJoined key = new ColumnsJoined(colLen);
            for (int k = 0; k < colLen; k++) {
                key.setColumn(k, PrimitiveSerializer.readForType(types[k], input));
            }

            // read value
            FieldFunctionState[] states = new FieldFunctionState[ffsLen];
            for (int f = 0; f < ffsLen; f++) {

                final FieldFunctionState state = FieldFunctionStates.forType(ffTypes[f]);
                state.read(input);
                states[f] = state;
            }

            list.put(key, states);
        }
        Stats.addMetric(READ_METRIC, (int) (System.currentTimeMillis() - start));
    }
}
