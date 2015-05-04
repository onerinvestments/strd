package strd.storage;

import strd.node.impl.databuffers.DataBufferVisitor;

/**
 * User: light
 * Date: 9/2/13
 * Time: 7:13 PM
 */
public interface MatrixBufferVisitor extends DataBufferVisitor {
    void visit( FieldFunctionState[] cols );
}
