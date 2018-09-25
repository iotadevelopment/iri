package com.iota.iri.service.milestone;

import com.iota.iri.model.Hash;

public class MilestoneSolidifierEntry {
    private Hash hash;

    private int index;

    public MilestoneSolidifierEntry(Hash hash, int index) {
        this.hash = hash;
        this.index = index;
    }

    public Hash getHash() {
        return hash;
    }

    public int getIndex() {
        return index;
    }
}
