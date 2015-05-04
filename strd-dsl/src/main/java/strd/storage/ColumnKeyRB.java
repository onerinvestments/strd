package strd.storage;

/**
* User: light
* Date: 03/12/13
* Time: 06:00
*/
public class ColumnKeyRB implements Comparable<ColumnKeyRB> {

    public final int idx;
    public final int streamId;

    public ColumnKeyRB(int idx, int streamId) {

        this.idx = idx;
        this.streamId = streamId;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        ColumnKeyRB that = (ColumnKeyRB) o;

        return streamId == that.streamId && idx == that.idx;
    }

    @Override
    public int hashCode() {
        int result = idx;
        result = 31 * result + streamId;
        return result;
    }

    @Override
    public final int compareTo(ColumnKeyRB combinatorKey) {
        return idx > combinatorKey.idx ? 1 : idx < combinatorKey.idx ? -1 : streamId > combinatorKey.streamId ? 1 : streamId < combinatorKey.streamId ? -1 : 0;
    }
}
