package com.redis.smartcache.core.config;

import java.util.Objects;

import io.lettuce.core.SslVerifyMode;

public class RedisConfig {

    private String uri;

    private boolean cluster;

    private boolean tls;

    private SslVerifyMode tlsVerify = SslVerifyMode.NONE;

    private String username;

    private char[] password;

    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
    }

    public SslVerifyMode getTlsVerify() {
        return tlsVerify;
    }

    public void setTlsVerify(SslVerifyMode tlsVerify) {
        this.tlsVerify = tlsVerify;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public boolean isCluster() {
        return cluster;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public char[] getPassword() {
        return password;
    }

    public void setPassword(char[] password) {
        this.password = password;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RedisConfig other = (RedisConfig) obj;
        return Objects.equals(uri, other.uri);
    }

}
