package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:20 PM
 */
public class HashRecordsGroupper implements RecordGroupper {

    private final int matrixLength;
    private final int[] keyColumns;
    private final HashMatrixBuffer buffer;


    private FieldFunctionState[] currentRecord;
    private ColumnsJoined currentKey;


    /**
     *
     * @param fns
     * @param keyColumns ids in selectSourceId
     */
    public HashRecordsGroupper( int matrixLength,
                                int[] keyColumns) {

        this.matrixLength = matrixLength;
        this.keyColumns = keyColumns;
        this.buffer = new HashMatrixBuffer();
    }

    @Override
    public void startRecord(SelectInputSource columnValues) {
        if (currentRecord != null) {
            throw new IllegalStateException();
        }

        int length = keyColumns.length;

        currentKey = new ColumnsJoined(length);
        for (int i = 0; i < length; i++) {
            int streamId = keyColumns[i];
//            Object o = columnValues.get(streamId);
            currentKey.columns[i] = columnValues.getForGroupper(streamId);
        }

        currentRecord = buffer.list.get(currentKey);

        if (currentRecord == null) {
            currentRecord = new FieldFunctionState[matrixLength];
        }
    }

    @Override
    public FieldFunctionState getRecordField(int colNum) {
        return currentRecord[colNum];
    }

    @Override
    public void setRecordField(int colNum, FieldFunctionState state) {
        currentRecord[colNum] = state;
    }

    @Override
    public void commitRecord() {
        if (currentKey == null) {
            throw new IllegalStateException();
        }

        buffer.list.put(currentKey, currentRecord);

        currentKey = null;
        currentRecord = null;
    }

    @Override
    public MatrixBuffer getBuffer() {
        return buffer;
    }

}
