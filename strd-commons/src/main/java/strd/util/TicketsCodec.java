package strd.util;

import lmbrd.zn.util.PrimitiveBits;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * User: light
 * Date: 14/04/14
 * Time: 06:18
 */
public class TicketsCodec {

    public static final String ALG = "RC4";

//    private final Logger log = LoggerFactory.getLogger(getClass());
    private byte[] buf = new byte[1024 * 16]; // 16KB MAXIMAL

    private Cipher encrypter;
    private Cipher decrypter;

    private Inflater inflater;
    private Deflater deflater;

    public TicketsCodec(String keyString) {
        try {

            SecretKeySpec key = new SecretKeySpec(keyString.getBytes(PrimitiveBits.UTF8_ENCODING), ALG);
            encrypter = Cipher.getInstance(ALG);
            decrypter = Cipher.getInstance(ALG);

            encrypter.init(Cipher.ENCRYPT_MODE, key);
            decrypter.init(Cipher.DECRYPT_MODE, key);

            inflater = new Inflater();

            deflater = new Deflater(Deflater.BEST_COMPRESSION);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public String encode(byte[] data, boolean compress) throws Exception {

        if (compress) {

            deflater.reset();
            deflater.setInput(data);
            deflater.finish();

            int compressedDataLength = deflater.deflate(buf);
            if (!deflater.finished()) {
                throw new RuntimeException("Not finished: " + compressedDataLength);
            }

            byte[] encrypted = encrypter.doFinal(buf, 0, compressedDataLength);


            return Base64.encodeBase64URLSafeString(encrypted);

        } else {
            byte[] encrypted = encrypter.doFinal(data);
            return Base64.encodeBase64URLSafeString(encrypted);

        }

    }

    public byte[] decode(String hash, boolean compress) throws Exception {
        byte[] base64 = Base64.decodeBase64(hash);
        byte[] encrypted = decrypter.doFinal(base64);

        if (compress) {

            inflater.reset();

            inflater.setInput(encrypted);
            int length = inflater.inflate(buf);

            if (!inflater.finished()) {
                throw new RuntimeException("Not finished");
            }

            byte[] result = new byte[length];
            System.arraycopy(buf, 0, result, 0, length);

            return result;
        } else {
            return encrypted;
        }
    }


}
