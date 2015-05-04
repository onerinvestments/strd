package strd;

/**
 * User: light
 * Date: 8/12/13
 * Time: 12:16 PM
 */
public class IndexId {
    public static final int TABLE_OFFSET = 16;
    public static final int COL_OFFSET = 0;

    public static final int TABLE_MASK = 0xFFFF << TABLE_OFFSET;
    public static final int COL_MASK = 0xFFFF;


    public int tableId;
    public int colId;

    public IndexId(int tableId, int colId) {
        this.colId = colId;
        this.tableId = tableId;
    }


    public int compress() {
        return ( tableId << TABLE_OFFSET) |
                (colId) ;

    }

    public static IndexId decompress(int id) {
        return new IndexId((id & TABLE_MASK) >>> TABLE_OFFSET,
                (id & COL_MASK) );
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexId indexId = (IndexId) o;

        if (colId != indexId.colId) return false;
        if (tableId != indexId.tableId) return false;

        return true;
    }

    public static int fetchIndexId( int id ) {
        return (id & COL_MASK);
    }

    public static int fetchTableId( int id ) {
        return (id & TABLE_MASK) >>> TABLE_OFFSET;
    }

    @Override
    public int hashCode() {
        int result = tableId;
        result = 31 * result + colId;
        return result;
    }

    @Override
    public String toString() {
        return "IndexId{" +
                "colId=" + colId +
                ", tableId=" + tableId +
                "} " + super.toString();
    }
}
