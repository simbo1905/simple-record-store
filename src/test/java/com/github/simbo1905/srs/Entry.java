package com.github.simbo1905.srs;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(includeFieldNames=true)
@EqualsAndHashCode
@Data(staticConstructor="of")
public class Entry<K,V> {
    final K key;
    final V value;
}
