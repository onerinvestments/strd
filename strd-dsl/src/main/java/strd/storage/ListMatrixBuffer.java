package strd.storage;

import strd.node.impl.databuffers.DataBufferVisitor;

import java.util.ArrayList;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:16 PM
 */
public class ListMatrixBuffer implements MatrixBuffer, StrdNodeResult {

    public ArrayList<FieldFunctionState[]> list = new ArrayList<>();

    @Override
    public void acceptVisitor(DataBufferVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void append(StrdNodeResult buf2, FieldDeclaration[] functions) {
        ArrayList<FieldFunctionState[]> otherList = ((ListMatrixBuffer) buf2).list;
        list.addAll(otherList);
    }

    @Override
    public void acceptVisitor(MatrixBufferVisitor visitor) {
        if (list.isEmpty()) {
            visitor.visitEmpty();
            return;
        }

        for (FieldFunctionState[] objects : list) {
            visitor.visit(objects);
        }
    }

    @Override
    public String toString() {
        return "ListMatrixBuffer{" +
                "list=" + list.size() +
                '}';
    }
}
