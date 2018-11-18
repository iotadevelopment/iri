package com.iota.iri.network;

import com.iota.iri.model.Hash;

public interface NetworkService {
    Hash getRandomTipHash();
}
