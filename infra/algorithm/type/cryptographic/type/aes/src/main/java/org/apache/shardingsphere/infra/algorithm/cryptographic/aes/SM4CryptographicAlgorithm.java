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

    private static final String SM4_KEY = "sm4-key-value";

    private Key secretKey;

    private byte[] iv;

    @Override
    public void init(final Properties props) {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        secretKey = getSecretKey(props);
        iv = generateIv();
    }

    private Key getSecretKey(final Properties props) {
        String key = props.getProperty(SM4_KEY);
        ShardingSpherePreconditions.checkNotEmpty(key, () -> new AlgorithmInitializationException(this, "%s can not be null or empty", SM4_KEY));
        return new SecretKeySpec(key.getBytes(), "SM4");
    }

    private byte[] generateIv() {
        return new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2};
    }

    @SneakyThrows(GeneralSecurityException.class)
    @Override
    public String encrypt(final Object plainValue) {
        if (null == plainValue) {
            return null;
        }
        Cipher cipher = Cipher.getInstance("SM4/CBC/PKCS7Padding", BouncyCastleProvider.PROVIDER_NAME);
        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParameterSpec);
        return Base64.getEncoder().encodeToString(cipher.doFinal(String.valueOf(plainValue).getBytes(StandardCharsets.UTF_8)));
    }

    @SneakyThrows(GeneralSecurityException.class)
    @Override
    public Object decrypt(final Object cipherValue) {
        if (null == cipherValue) {
            return null;
        }
        Cipher cipher = Cipher.getInstance("SM4/CBC/PKCS7Padding", BouncyCastleProvider.PROVIDER_NAME);
        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParameterSpec);
        return new String(cipher.doFinal(String.valueOf(cipherValue).getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }


    @Override
    public String getType() {
        return "SM4";
    }
}
