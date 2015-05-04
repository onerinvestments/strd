package strd.storage;

import lmbrd.zn.util.PrimitiveBits;

import java.io.OutputStream;
import java.util.Arrays;

/**
 * User: light
 * Date: 9/2/13
 * Time: 3:27 PM
 */
public class ColumnsJoined {

    public final Object[] columns;
    public final int colNum;

    public ColumnsJoined(int colNum) {
        this.colNum = colNum;
        this.columns = new Object[colNum];
    }

    public void write(OutputStream os) {
        for (Object column : columns) {
            PrimitiveSerializer.write(column, os);
        }
    }

    public void read(PrimitiveBits.DataBuf buf, int sourceStreamsCount) {
        for (int i = 0; i< sourceStreamsCount; i++) {
            Object read = PrimitiveSerializer.read(buf);
            if(i < colNum)
                columns[i] = read;
        }
    }

    public void clear() {
        for (int i = 0; i < colNum; i++){
            columns[i] = null;
        }
    }

    public void setColumn(int fieldNum, Object value) {
//        empty = false;
        this.columns[fieldNum] = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        ColumnsJoined that = (ColumnsJoined) o;

//        if (colNum != that.colNum) return false;
        if (!Arrays.equals(columns, that.columns)) return false;

        return true;
    }

    public int estimatedSize() {
        int size = columns.length * 8 + 8;

        for (Object column : columns) {
            if (column != null) {
                if (column instanceof Integer) {
                    size += 4;
                } else if (column instanceof Long) {
                    size += 8;
                } else if (column instanceof String) {
                    size += ((String) column).length() * 2;
                } else {
                    System.out.println("Unknown column value: " + column.getClass().getName());
                }
            }
        }

        return size;
    }


    @Override
    public int hashCode() {
        return Arrays.hashCode(columns);
    }

    public int getIntColumn(int colNum) {
        return (int) columns[colNum];
    }

    public long getLongColumn(int colNum) {
        return (long) columns[colNum];
    }
    public Object getColumn(int colNum) {
        return columns[colNum];
    }

    @Override
    public String toString() {
        return "ColumnsJoined{" +
                "columns=" + Arrays.toString(columns) +
                '}';
    }
}
