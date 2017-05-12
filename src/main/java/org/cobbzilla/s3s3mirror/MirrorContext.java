package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.Owner;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class MirrorContext {

    @Getter @Setter private MirrorOptions options;
    @Getter @Setter private Owner owner;
    @Getter private final MirrorStats stats = new MirrorStats();

}
