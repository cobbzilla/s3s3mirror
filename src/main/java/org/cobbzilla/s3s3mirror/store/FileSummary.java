package org.cobbzilla.s3s3mirror.store;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@AllArgsConstructor @Accessors(chain=true)
public class FileSummary {

    public FileSummary() {
    }

    /** a unique string that identifies this file */
    @Getter @Setter private String key;

    /** the size of the file, in bytes */
    @Getter @Setter private long size;

    /** last-modified time, in milliseconds since the UNIX epoch, or null if it cannot be determined */
    @Getter @Setter private Long lastModified;

    /** an ETag associated with the file, or null if no ETag is present */
    @Getter @Setter private String eTag;

    /** the SHA-256 hash of the local file, or the value found in remote S3 ObjectMetadata with the key s3s3-sha256 */
    @Getter @Setter private String sha256;

    /** if this represents a symlink, this is the path to the link target, or null if not a symlink */
    @Getter @Setter private String linkTarget;

    public boolean isSymlink() { return linkTarget != null; }

}
