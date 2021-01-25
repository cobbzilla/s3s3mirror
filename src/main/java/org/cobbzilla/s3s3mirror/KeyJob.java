package org.cobbzilla.s3s3mirror;

public interface KeyJob extends Runnable {
    String getSource();
    String getDestination();
}
