package org.cobbzilla.s3s3mirror.store.local;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

@Slf4j
public class StdinFileIterator implements Iterator<File> {

    private final BufferedReader reader;
    private String line;

    public StdinFileIterator () {
        reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            line = reader.readLine();
        } catch (IOException e) {
            log.error("error reading first line: "+e, e);
            line = null;
        }
    }

    @Override public boolean hasNext() { return line != null; }

    @Override public File next() {
        final File file = new File(line);
        try {
            line = reader.readLine();
        } catch (IOException e) {
            throw new IllegalStateException("Error reading from stdin: "+e, e);
        }
        return file;
    }

    @Override public void remove() { throw new IllegalStateException("remove not supported"); }

}
