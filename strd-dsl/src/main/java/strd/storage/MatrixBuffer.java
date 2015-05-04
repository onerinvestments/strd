package strd.storage;

import strd.node.impl.databuffers.DataBuffer;

/**
 * User: light
 * Date: 9/2/13
 * Time: 5:28 PM
 */
public interface MatrixBuffer extends DataBuffer {
    void append( StrdNodeResult buf2, FieldDeclaration[] functions );

    void acceptVisitor( MatrixBufferVisitor visitor );
}
