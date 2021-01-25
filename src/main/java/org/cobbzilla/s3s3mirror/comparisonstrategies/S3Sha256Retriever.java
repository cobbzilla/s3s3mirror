package org.cobbzilla.s3s3mirror.comparisonstrategies;

@FunctionalInterface
public interface S3Sha256Retriever {
    String getSha(String key) throws Exception;
}
