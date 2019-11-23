package com.github.trex_paxos.srs;

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
