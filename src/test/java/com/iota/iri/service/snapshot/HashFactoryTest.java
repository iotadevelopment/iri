package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HashFactoryTest {
    @Test
    public void testHashFactory() {
        Map<Hash, Long> balances = new HashMap<>();

        balances.put(HashFactory.ADDRESS.create("NPWEYELYMJZRLJSVLHPTOZDERNSQD9ASONVQWIRVNVTVATQUWNHDAOVBDVDKRXSMJDROAGEDRHEZONPPW"), 12L);

        Assert.assertEquals("test", (long) balances.get(HashFactory.ADDRESS.create("NPWEYELYMJZRLJSVLHPTOZDERNSQD9ASONVQWIRVNVTVATQUWNHDAOVBDVDKRXSMJDROAGEDRHEZONPPW")), 12L);
    }
}
