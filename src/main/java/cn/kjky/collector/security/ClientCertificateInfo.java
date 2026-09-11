package cn.kjky.collector.security;

import java.time.Instant;

/** 可安全输出的客户端证书摘要，不包含证书正文、私钥或密码。 */
public record ClientCertificateInfo(
        String fingerprintSha256,
        String subjectSummary,
        Instant notBefore,
        Instant notAfter
) {
}
