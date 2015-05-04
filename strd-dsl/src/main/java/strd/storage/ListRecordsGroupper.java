package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 3:48 PM
 */
public class ListRecordsGroupper implements RecordGroupper {

    private ListMatrixBuffer buffer = new ListMatrixBuffer();

    private final int matrixLength;
    private FieldFunctionState[] currentRecord;

    public ListRecordsGroupper(int matrixLength) {
        this.matrixLength = matrixLength;
    }

    @Override
    public void startRecord(SelectInputSource columnValues) {
        if (currentRecord != null) {
            throw new IllegalStateException();
        }

        currentRecord = new FieldFunctionState[matrixLength];
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
        if (currentRecord == null) {
            throw new NullPointerException();
        }
        buffer.list.add(currentRecord);
        currentRecord = null;
    }

    public MatrixBuffer getBuffer() {
        return buffer;
    }
}
