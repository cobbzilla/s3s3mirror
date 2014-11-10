package org.cobbzilla.s3s3mirror.store;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor @ToString
public class ListRequest {

    @Getter @Setter private String bucket;
    @Getter @Setter private String prefix;
    @Getter @Setter private int fetchSize;


}
