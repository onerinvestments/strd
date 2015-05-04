package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 3:46 PM
 */
public interface RecordGroupper {

    void startRecord(SelectInputSource inputs);

    FieldFunctionState getRecordField(int colNum);

    void setRecordField(int colNum, FieldFunctionState state);

    void commitRecord();

    MatrixBuffer getBuffer();

}
