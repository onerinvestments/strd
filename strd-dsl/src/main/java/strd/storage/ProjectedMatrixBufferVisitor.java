package strd.storage;

/**
 * User: light
 * Date: 01/12/13
 * Time: 07:09
 */
public interface ProjectedMatrixBufferVisitor {
    void visit( byte[] key, FieldFunctionStates buf);
}
