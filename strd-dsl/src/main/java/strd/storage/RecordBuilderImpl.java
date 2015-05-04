package strd.storage;

/**
 * User: light
 * Date: 03/12/13
 * Time: 16:57
 */
public class RecordBuilderImpl {

    private final RecordGroupper groupper;
    private final FieldDeclaration[] functions;

    private final RecordFieldSource2[] sources;


    private final int recordsCount;

    private final int sourcesLength;

    public RecordBuilderImpl(FieldDeclaration[] functions,
                             RecordGroupper groupper,
                             RecordFieldSource2[] sources,
                             int recordsCount) {

        this.functions = functions;
        this.groupper = groupper;
        this.sources = sources;

        this.recordsCount = recordsCount;
        this.sourcesLength = sources.length;

        if (recordsCount < 0 || sourcesLength <= 0) {
            throw new IllegalArgumentException();
        }
    }

    public void execute() {

//        int currentIndex = 0;

        SelectInputSource sourceValues = new SelectInputSource(sourcesLength, functions);

//        TIntObjectHashMap columnValues =  new TIntObjectHashMap( sourcesLength );
//        System.out.println("-->ExecuteBuilder: " + recordsCount +" / ins:" + sourcesLength + "/ fns:" + functions.length);

        int index = Integer.MAX_VALUE;

        for (int srcIdx = 0; srcIdx < sourcesLength; srcIdx++) {
            final RecordFieldSource2 source = sources[srcIdx];
            if (index > source.currentIndex && source.currentIndex != -1) {
                index = source.currentIndex;
            }
        }

        while (index < recordsCount) {
            index = processIndex(sourceValues, index);
        }
//        System.out.println("<--ExecuteBuilder");
    }

    private int processIndex(SelectInputSource columnValues, int index) {
        int minNextIndex = Integer.MAX_VALUE;
        int fetched = 0;

        for (int srcIdx = 0; srcIdx < sourcesLength; srcIdx++) {
            final RecordFieldSource2 source = sources[srcIdx];

            if (source.currentIndex == index) {
                columnValues.set( source.selectSourceId, source.getValue() );
                fetched++;

                int nextInSource = source.next();

                if (nextInSource != -1) {
                    if (minNextIndex > nextInSource) {
                        minNextIndex = nextInSource;
                    }
                }
            }
        }

        if (fetched < sourcesLength) {
//            if ( fetched > 0 ){
                columnValues.clear();
//            }

            return minNextIndex;
        }

        groupper.startRecord(columnValues);

        for (FieldDeclaration function : functions) {
            FieldFunction fn = function.fn;
//            boolean status = true;


            FieldFunctionState state = groupper.getRecordField(function.matrixOutputNum);
            if (state == null) {
                state = fn.init();
            }

            boolean append = true;

            for (FieldFilterImpl filter : function.filters) {
                if (!filter.isIncludes(columnValues)) {
                    append = false;
                    break;
                }
            }

            if (append)
                fn.append(columnValues, state);

            groupper.setRecordField(function.matrixOutputNum, state);
        }

        groupper.commitRecord();
        columnValues.clear();

        return minNextIndex;
    }
}
