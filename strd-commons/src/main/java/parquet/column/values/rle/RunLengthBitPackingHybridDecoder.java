/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.rle;

import parquet.Log;
import parquet.Preconditions;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;
import parquet.io.ParquetDecodingException;
import strd.bytes.InputBytes;

import java.io.IOException;

import static parquet.Log.DEBUG;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 *
 * @author Julien Le Dem
 */
public class RunLengthBitPackingHybridDecoder {
    private static final Log LOG = Log.getLog(RunLengthBitPackingHybridDecoder.class);

    private static enum MODE {RLE, PACKED}

    private final int bitWidth;
    private final BytePacker packer;
    private final InputBytes in;

    private MODE mode;

    private int currentCount;
    private int currentValue;
    private int[] currentBuffer;

    public RunLengthBitPackingHybridDecoder(int bitWidth, InputBytes in) {
        if (DEBUG) LOG.debug("decoding bitWidth " + bitWidth);

        Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
        this.bitWidth = bitWidth;
        this.packer = ByteBitPackingLE.getPacker(bitWidth);
        this.in = in;
    }

    public static int[] toIntArray(int count, int bw, InputBytes input) throws IOException {
        int[] array = new int[count];
        RunLengthBitPackingHybridDecoder recs = new RunLengthBitPackingHybridDecoder(bw, input);
        for (int i = 0; i<count;i++) {
            array[i] = recs.readInt();
        }
        return array;
    }

    public static short[] toShortArray(int count, int bw, InputBytes input) throws IOException {
        short[] array = new short[count];
        RunLengthBitPackingHybridDecoder recs = new RunLengthBitPackingHybridDecoder(bw, input);
        for (int i = 0; i<count;i++) {
            array[i] = (short) recs.readInt();
        }
        return array;
    }

    public int readInt() throws IOException {
        if (currentCount == 0) {
            readNext();
        }
        --currentCount;
        int result;
        switch (mode) {
            case RLE:
                result = currentValue;
                break;
            case PACKED:
                result = currentBuffer[currentBuffer.length - 1 - currentCount];
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
        return result;
    }


    private void readNext() throws IOException {
        final int header = in.readUnsignedVarInt();
        mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (mode) {
            case RLE:
                currentCount = header >>> 1;
                if (DEBUG) LOG.debug("reading " + currentCount + " values RLE");
                currentValue = in.readIntLittleEndianPaddedOnBitWidth(bitWidth);
                        //BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
                break;
            case PACKED:
                int numGroups = header >>> 1;
                currentCount = numGroups * 8;
                if (DEBUG) LOG.debug("reading " + currentCount + " values BIT PACKED");
                currentBuffer = new int[currentCount]; // TODO: reuse a buffer
                byte[] bytes = new byte[numGroups * bitWidth];

                in.readBytes( bytes );

                for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += bitWidth) {
                    packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
                }
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
    }
}
