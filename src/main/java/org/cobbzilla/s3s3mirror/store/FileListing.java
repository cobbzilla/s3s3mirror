package org.cobbzilla.s3s3mirror.store;

import java.util.List;

public interface FileListing {

    public List<FileSummary> getFileSummaries();

    public boolean hasMore();

}
