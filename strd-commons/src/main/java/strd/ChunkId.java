package strd;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static lmbrd.zn.util.PrimitiveBits.*;

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/9/13
 * Time: 6:09 PM
 */
public class ChunkId {
    public static final int PROJECTION_TABLE_ID = 1000;

    public static final int TABLE_OFFSET = 48;
    public static final int AGG_OFFSET = 40;
    public static final int SEQ_OFFSET = 8;
    public static final int VER_OFFSET = 0;

    public static final long TABLE_MASK = 0xFFFFL << TABLE_OFFSET;
    public static final long AGG_MASK = 0xFFL << AGG_OFFSET;
    public static final long SEQ_MASK = 0xFFFFFFFFL << SEQ_OFFSET;
    public static final long VER_MASK = 0xFFL << VER_OFFSET;


    public final int table;
    public final int agg;
    public final long seq;
    public final int ver;


    public ChunkId(int table, int agg, long seq, int ver) {
        this.ver = ver;
        if (table < 0 || table > Short.MAX_VALUE * 2 + 1) {
            throw new IllegalArgumentException("table " + table);
        }
        if (agg < 0 || agg > Byte.MAX_VALUE * 2 + 1) {
            throw new IllegalArgumentException("agg " + agg);
        }
        if (seq < 0 || seq > 2L * Integer.MAX_VALUE + 1L) {
            throw new IllegalArgumentException("seq " + seq);
        }

        this.table = table;
        this.agg = agg;
        this.seq = seq;
    }

    public boolean isProjected() {
        return table > 1000;
    }

    public static boolean isSourceChunk(long id ) {
        return decompress(id).agg == 0;
    }

    public static boolean isJoinedChunk(long id ) {
        return decompress(id).agg == 1;
    }

    public static boolean isProjectedChunk(long id) {
        return decompress(id).table > 1000;
    }

    public long compress() {
        return (((long) table) << TABLE_OFFSET) |
                (((long) agg) << AGG_OFFSET) |
                (seq << SEQ_OFFSET) |
                (((long) ver) << VER_OFFSET);
    }

    public static int getTableId(long id) {
        return (int) ((id & TABLE_MASK) >>> TABLE_OFFSET);
    }

    public static ChunkId decompress(long id) {
        return new ChunkId((int) ((id & TABLE_MASK) >>> TABLE_OFFSET),
                (int) ((id & AGG_MASK) >>> AGG_OFFSET),
                ((id & SEQ_MASK) >>> SEQ_OFFSET),
                (int) ((id & VER_MASK) >>> VER_OFFSET));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChunkId chunkId = (ChunkId) o;

        if (agg != chunkId.agg) return false;
        if (seq != chunkId.seq) return false;
        if (table != chunkId.table) return false;
        if (ver != chunkId.ver) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = table;
        result = 31 * result + agg;
        result = 31 * result + (int) (seq ^ (seq >>> 32));
        result = 31 * result + ver;
        return result;
    }

    @Override
    public String toString() {
        return "ChunkId{" +
                "table=" + table +
                ", agg=" + agg +
                ", seq=" + seq +
                ", ver=" + ver +
                ", id=" + compress() +
                '}';
    }

    public static void main(String[] args) throws ParseException {
        System.out.println( ChunkId.decompress(282574488338688L));

        System.out.println(new SimpleDateFormat("dd.MM.yyyy").parse( "24.03.2013").getTime());
        System.out.println(new SimpleDateFormat("dd.MM.yyyy").parse( "29.03.2014").getTime());
        System.out.println(new SimpleDateFormat("dd.MM.yyyy").parse( "28.03.2014").getTime());

        System.out.println(DateFormat.getDateTimeInstance().format(new Date(1391933962150L)));
         // 282322700175694080,1003,114724416,192198786,1265127
        System.out.println(ChunkId.decompress(new ChunkId(1, 1, 1, 1).compress()));
        System.out.println(Long.toBinaryString(new ChunkId(0, 0, 0, 1).compress()));
        System.out.println(Long.toBinaryString(TABLE_MASK));
        System.out.println(Long.toBinaryString(AGG_MASK));
        System.out.println(Long.toBinaryString(SEQ_MASK));
        System.out.println(Long.toBinaryString((((long) compressShort(4)) << TABLE_OFFSET)));
        System.out.println(Long.toBinaryString((((long) compressByte(1)) << AGG_OFFSET)));
        System.out.println(Long.toBinaryString((compressInt(1) << SEQ_OFFSET)));

        System.out.println(new ChunkId(1, 1, 1, 1).equals(ChunkId.decompress(new ChunkId(1, 1, 1, 1).compress())));
        System.out.println(new ChunkId(44444, 250, Integer.MAX_VALUE + 1L, 5).equals(ChunkId.decompress(new ChunkId(44444, 250, Integer.MAX_VALUE + 1L, 5).compress())));

        System.out.println(ChunkId.decompress(new ChunkId(44444, 250, Integer.MAX_VALUE + 1L, 5).compress()));

        System.out.println(Short.MAX_VALUE);

    }



    /*
    * 1111111111111111000000000000000000000000000000000000000000000000
                      111111110000000000000000000000000000000000000000
                              1111111111111111111111111111111100000000
    *
    *
    *
    * */

}
