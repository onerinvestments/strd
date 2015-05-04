package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:20 PM
 */

public class ProjectedRecordsGroupper implements RecordGroupper {

    private final int projSelectInputs;

    private final ProjectedKeyBuilder keyBuilder;

    private ProjectedMatrixBuffer buffer = new ProjectedMatrixBuffer();

    private FieldFunctionStates currentRecord;

    private byte[] projKey;

    public ProjectedRecordsGroupper( int projSelectInputs,      // count (zero-based) for projection inputs (includes project additional streams)
                                     ProjectedKeyBuilder keyBuilder ) {

        this.projSelectInputs = projSelectInputs;
        this.keyBuilder = keyBuilder;
    }

    @Override
    public void startRecord(SelectInputSource columnValues) {
        if (currentRecord != null) {
            throw new IllegalStateException();
        }

        projKey = buildProjectionKey(columnValues);
        currentRecord = buffer.getRow( projKey );

        if (currentRecord == null) {
            currentRecord = new FieldFunctionStates( projSelectInputs );
        }
    }

    public byte[] buildProjectionKey(SelectInputSource columnValues) {
        return keyBuilder.buildKey(columnValues);
    }

    @Override
    public FieldFunctionState getRecordField(int colNum) {
        return currentRecord.get(colNum);
    }

    @Override
    public void setRecordField(int colNum, FieldFunctionState state) {
        currentRecord.set(colNum, state);
    }

    @Override
    public void commitRecord() {

        buffer.putRow( projKey, currentRecord );

        projKey = null;
        currentRecord = null;

    }

    @Override
    public MatrixBuffer getBuffer() {
        return buffer;
    }

}
                             /*
\x03\xE9\x03\x00\x00\x00\x01\x00\ x00\x00\x00\x0D\ x00\x00\x01B\xDF*\xE6\x80
\x03\xE9\x03\x00\x00\x00\x01\x00\ x00\x00\x00\x0D\ x00\x00\x00\x08\x00!kdfjghlsdfkjhg lsdkjfhg lsdkfjh29\x00\x00\x01B\xDE\xF3\xF8\x00
\x03\xE9\x03\x00\x00\x00\x01\x00\ x00\x00\x00\x0D\ x00\x00\x01B\xDF\xCF\xB2\x00

                              */