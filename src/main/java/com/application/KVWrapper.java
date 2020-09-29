package com.application;

import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.Objects;

public class  KVWrapper implements Serializable {
    KV<Boolean,Long> value;

    public KVWrapper( KV<Boolean,Long> value){
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KVWrapper kvWrapper = (KVWrapper) o;
        return Objects.equals(value, kvWrapper.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}