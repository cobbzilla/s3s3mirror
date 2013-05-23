package org.cobbzilla.s3s3mirror;

import lombok.Getter;
import lombok.Setter;

public class MirrorContext {

    @Getter @Setter private MirrorOptions options;
    @Getter @Setter private MirrorStats stats = new MirrorStats();

    public MirrorContext (MirrorOptions options) {
        this.options = options;
    }

}
