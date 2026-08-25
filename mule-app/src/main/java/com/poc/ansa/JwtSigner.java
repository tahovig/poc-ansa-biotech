package com.poc.ansa;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Base64;

public class JwtSigner {

    public static String sign(String consumerKey, String username, String audience,
                               String keystorePath, String keystorePassword, String keyAlias) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        // keystorePath (e.g. "keys/sfdc-jwt.p12") is packaged onto the app's
        // classpath under src/main/resources, not left as a loose file next
        // to the runtime's working directory -- load it as a classpath
        // resource, not via FileInputStream, or this throws
        // FileNotFoundException in the real deployment (Task 12's Dockerfile
        // only copies the built jar, not the source tree).
        try (InputStream is = JwtSigner.class.getClassLoader().getResourceAsStream(keystorePath)) {
            if (is == null) {
                throw new IOException("Keystore not found on classpath: " + keystorePath);
            }
            keyStore.load(is, keystorePassword.toCharArray());
        }
        PrivateKey privateKey = (PrivateKey) keyStore.getKey(keyAlias, keystorePassword.toCharArray());

        long exp = (System.currentTimeMillis() / 1000L) + 300L;
        String header = "{\"alg\":\"RS256\"}";
        String claims = String.format(
            "{\"iss\":\"%s\",\"sub\":\"%s\",\"aud\":\"%s\",\"exp\":%d}",
            consumerKey, username, audience, exp);

        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String headerEncoded = encoder.encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String claimsEncoded = encoder.encodeToString(claims.getBytes(StandardCharsets.UTF_8));
        String signingInput = headerEncoded + "." + claimsEncoded;

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(signingInput.getBytes(StandardCharsets.UTF_8));
        byte[] signed = signature.sign();
        String signatureEncoded = encoder.encodeToString(signed);

        return signingInput + "." + signatureEncoded;
    }
}
