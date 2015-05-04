package javaewah;

/*
* Copyright 2009-2011, Daniel Lemire
* Licensed under the GPL version 3 and APL 2.0, among other licenses.
*/

public final class EWAHIterator {
    private final EWAHCompressedBitmap bitmap;
    RunningLengthWord rlw;
    int size;
    int pointer;

    //public int[] intsBuffer = new int[16384];

    public EWAHIterator(final long[] a, final int sizeinwords, EWAHCompressedBitmap bitmap) {

        this.bitmap = bitmap;
        this.rlw = new RunningLengthWord(a,0);
        this.size = sizeinwords;
        this.pointer = 0;
    }

/*
    public void clear() {
        if (bitmap == null) {
            return;
        }
        this.pointer = 0;

        bitmap.releaseIterator( this );
    }
*/

	public boolean hasNext() {
        return this.pointer<this.size;
    }

    public RunningLengthWord next() {
        this.rlw.position = this.pointer;
        this.pointer += this.rlw.getNumberOfLiteralWords() + 1;
        return this.rlw;
    }

    public int dirtyWords()  {
        return this.pointer-(int)this.rlw.getNumberOfLiteralWords();
    }

    public long[] buffer() {
        return this.rlw.array;
    }

}
