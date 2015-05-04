package strd.storage;

/**
 * User: light
 * Date: 01/12/13
 * Time: 08:26
 */
public interface MatrixKeyValueVisitor {
    void visit( ColumnsJoined key, FieldFunctionState[] cols);
}
