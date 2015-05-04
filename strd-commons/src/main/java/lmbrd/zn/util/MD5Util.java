package lmbrd.zn.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * User: light
 * Date: 9/29/13
 * Time: 1:32 AM
 */
public class MD5Util {
    static private final int BASELENGTH = 128;
    static private final int LOOKUPLENGTH = 16;
    static final private byte[] hexNumberTable = new byte[BASELENGTH];
    static final private char[] lookUpHexAlphabet = new char[LOOKUPLENGTH];


    static {
        for (int i = 0; i < BASELENGTH; i++) {
            hexNumberTable[i] = -1;
        }
        for (int i = '9'; i >= '0'; i--) {
            hexNumberTable[i] = (byte) (i - '0');
        }
        for (int i = 'F'; i >= 'A'; i--) {
            hexNumberTable[i] = (byte) (i - 'A' + 10);
        }
        for (int i = 'f'; i >= 'a'; i--) {
            hexNumberTable[i] = (byte) (i - 'a' + 10);
        }

        for (int i = 0; i < 10; i++) {
            lookUpHexAlphabet[i] = (char) ('0' + i);
        }
        for (int i = 10; i <= 15; i++) {
            lookUpHexAlphabet[i] = (char) ('A' + i - 10);
        }
    }


    /**
     * Encode a byte array to hex string
     *
     * @param binaryData array of byte to encode
     * @return return encoded string
     */
    static public String convertToHex(byte[] binaryData, int off, int len) {
        if (binaryData == null)
            throw new NullPointerException();

        int lengthEncode = len * 2;

        char[] encodedData = new char[lengthEncode];
        int temp;
        for (int i = off; i < len + off; i++) {
            temp = binaryData[i];
            if (temp < 0)
                temp += 256;
            encodedData[i * 2] = lookUpHexAlphabet[temp >> 4];
            encodedData[i * 2 + 1] = lookUpHexAlphabet[temp & 0xf];
        }
        return new String(encodedData);
    }

    static public String convertToHex(byte[] binaryData) {
        return convertToHex(binaryData, 0, binaryData.length);
    }

    /**
     * Decode hex string to a byte array
     *
     * @param encoded encoded string
     * @return return array of byte to encode
     */
    static public byte[] fromHex(String encoded) {
        if (encoded == null)
            throw new NullPointerException();

        int lengthData = encoded.length();

        if (lengthData % 2 != 0)
            throw new IllegalStateException("bad string :" + encoded);

        char[] binaryData = encoded.toCharArray();
        int lengthDecode = lengthData / 2;
        byte[] decodedData = new byte[lengthDecode];
        byte temp1, temp2;
        char tempChar;
        for (int i = 0; i < lengthDecode; i++) {
            tempChar = binaryData[i * 2];
            temp1 = (tempChar < BASELENGTH) ? hexNumberTable[tempChar] : -1;
            if (temp1 == -1)
                throw new IllegalStateException("bad string :" + encoded);
            tempChar = binaryData[i * 2 + 1];
            temp2 = (tempChar < BASELENGTH) ? hexNumberTable[tempChar] : -1;
            if (temp2 == -1)
                throw new IllegalStateException("bad string :" + encoded);
            decodedData[i] = (byte) ((temp1 << 4) | temp2);
        }
        return decodedData;
    }
/*


    public static byte[] fromHex(String str) {
        char[] chars = str.toCharArray();
        byte[] bytes = new byte[chars.length / 2];

        int v;
        for (int j = 0; j < bytes.length; j++) {


            v = data[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }

    }

    public static String convertToHex(byte[] data) {

        char[] hexChars = new char[data.length * 2];
        int v;
        for (int j = 0; j < data.length; j++) {
            v = data[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);

*/


/*
        StringBuilder hexString = new StringBuilder();
        for (byte aData : data) {
            hexString.append(Integer.toHexString(0xFF & aData));

        }
        return hexString.toString();

        return new BigInteger(data).toString(16);
    }
*/

    public static String md5asString(String str) {
        return md5asString(str.getBytes(BytesHash.UTF8));
    }

    public static String md5asString(byte[] ba) {
        byte[] digest = md5Bytes(ba);
        return convertToHex(digest);
    }

    private static final ThreadLocal<MessageDigest> digester = new ThreadLocal<MessageDigest>() {
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
    };

    public static MessageDigest getMD5() {
        MessageDigest messageDigest = digester.get();
        messageDigest.reset();
        return messageDigest;
    }

    public static byte[] md5Bytes(byte[] ba) {
        return md5Bytes(ba, 0, ba.length);
    }

    public static byte[] md5Bytes(byte[] ba, int off, int len) {
        MessageDigest m = getMD5();
        m.reset();
        m.update(ba, off, len);

        return m.digest();
    }

    public static byte[] md5Bytes(String str) {
        return md5Bytes(str.getBytes(BytesHash.UTF8));
    }
}
