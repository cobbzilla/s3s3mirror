package org.cobbzilla.s3s3mirror.store.local;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;

@Slf4j
public class LocalFileIterator implements Iterator<File> {

    private Stack<ListPosition> dirs = new Stack<ListPosition>();
    private List<String> prefix = new ArrayList<String>();
    private ListPosition current;
    private int level = 0;
    private File nextFile;

    public LocalFileIterator(File base, String prefix) {
        current = new ListPosition(base);
        dirs.push(current);
        if (prefix != null) {
            for (String part : prefix.split(File.separator)) {
                if (part.length() > 0) this.prefix.add(part);
            }
        }
        nextFile = fetch();
    }

    @Override public boolean hasNext() { return nextFile != null; }

    @Override public File next() {
        final File f = nextFile;
        nextFile = fetch();
        return f;
    }

    public File fetch() {
        if (current.index >= current.listing.length) {
            if (dirs.empty()) return null;

            current = dirs.pop();
            level--;
            if (level < 0) return null;
            return fetch();
        }

        final File file = current.listing[current.index++];

        if (!prefixMatch(file)) {
            return fetch();
        }

        if (file.isDirectory()) {
            dirs.push(current);
            current = new ListPosition(file);
            level++;
            return fetch();
        }

        return file;
    }

    private boolean prefixMatch(File file) {
        if (level < 0) return true; // top-level?

        // beyond where matching matters, copy everything
        if (level >= prefix.size()) return true;

        // at the last level, look for filename prefix match
        if (level == prefix.size() - 1)  return file.getName().startsWith(prefix.get(prefix.size() - 1));

        // at a middle level, ensure we maintain a match at this level
        return file.getName().equals(prefix.get(level));
    }

    @Override public void remove() { throw new IllegalStateException("remove not supported"); }

    private class ListPosition {
        public File[] listing;
        public int index = 0;

        public ListPosition(File base) { listing = base.listFiles(); }
    }

    // for testing/fun
    public static void main (String[] args) throws Exception {
        final String prefix = args.length > 1 ? args[1] : null;
        final LocalFileIterator iterator = new LocalFileIterator(new File(args[0]), prefix);
        while (iterator.hasNext()) {
            System.out.println(iterator.next().getAbsolutePath());
        }
    }
}
