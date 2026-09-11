package cn.kjky.collector.testbed.cc;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.HexFormat;

record ClientCertificateIdentity(String fingerprintSha256, String subjectSummary, Instant notAfter) {
    static ClientCertificateIdentity from(X509Certificate certificate) {
        try {
            String fingerprint = HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
            return new ClientCertificateIdentity(fingerprint, commonName(certificate), certificate.getNotAfter().toInstant());
        } catch (Exception e) {
            throw new IllegalStateException("Cannot inspect peer certificate", e);
        }
    }

    private static String commonName(X509Certificate certificate) {
        try {
            for (Rdn rdn : new LdapName(certificate.getSubjectX500Principal().getName()).getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) return "CN=" + rdn.getValue();
            }
        } catch (Exception ignored) {
            // Fall through to a non-sensitive generic label.
        }
        return "CN=unavailable";
    }
}
