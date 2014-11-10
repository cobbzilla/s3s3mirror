package org.cobbzilla.s3s3mirror.store;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@NoArgsConstructor @AllArgsConstructor @Accessors(chain=true)
public class FileSummary {

    /** a unique string that identifies this file */
    @Getter @Setter private String key;

    /** the size of the file, in bytes */
    @Getter @Setter private long size;

    /** last-modified time, in milliseconds since the UNIX epoch, or null if it cannot be determined */
    @Getter @Setter private Long lastModified;

    /** an ETag associated with the file, or null if no ETag is present */
    @Getter @Setter private String eTag;

}
