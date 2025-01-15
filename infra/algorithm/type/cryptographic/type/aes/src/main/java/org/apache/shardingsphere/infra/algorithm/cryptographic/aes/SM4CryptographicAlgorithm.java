package org.apache.shardingsphere.infra.algorithm.cryptographic.aes;

import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.Security;
import java.util.Base64;
import java.util.Properties;

/**
 * SM4 cryptographic algorithm with CBC/PKCS7Padding.
 */
public final class SM4CryptographicAlgorithm implements CryptographicAlgorithm {

    private static final String ALGORITHM = "SM4/CBC/PKCS7Padding";
    private static final String PROVIDER = BouncyCastleProvider.PROVIDER_NAME;
    private static final String SM4_KEY = "sm4-key-value";

    private Key secretKey;
    private byte[] iv;

    @Override
    public void init(final Properties props) {
        Security.addProvider(new BouncyCastleProvider());
        secretKey = getSecretKey(props);
        iv = generateIv();
    }

    private Key getSecretKey(final Properties props) {
        String key = props.getProperty(SM4_KEY);
        ShardingSpherePreconditions.checkNotEmpty(key, () -> new AlgorithmInitializationException(this, "%s can not be null or empty", SM4_KEY));
        return new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "SM4");
    }

    private byte[] generateIv() {
        return new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2};
    }

    @Override
    public String encrypt(final Object plainValue) {
        if (plainValue == null || String.valueOf(plainValue).trim().isEmpty()) {
            return null;
        }
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParameterSpec);
            byte[] encryptedBytes = cipher.doFinal(String.valueOf(plainValue).getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Encryption failed", e);
        }
    }

    @Override
    public Object decrypt(final Object cipherValue) {
        if (cipherValue == null || String.valueOf(cipherValue).trim().isEmpty()) {
            return null;
        }
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParameterSpec);
            byte[] decodedBytes = Base64.getDecoder().decode(String.valueOf(cipherValue));
            byte[] decryptedBytes = cipher.doFinal(decodedBytes);
            return new String(decryptedBytes, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Decryption failed", e);
        }
    }

    @Override
    public String getType() {
        return "SM4";
    }
}
