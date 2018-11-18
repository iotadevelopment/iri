package com.iota.iri.network.impl;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.model.Hash;
import com.iota.iri.network.NetworkService;

import java.security.SecureRandom;

public class NetworkServiceImpl implements NetworkService {
    private final SecureRandom random = new SecureRandom();

    private final IotaConfig config;

    public NetworkServiceImpl(IotaConfig config) {
        this.config = config;
    }

    public Hash getRandomTipHash() throws Exception {
        Hash tip = random.nextDouble() < config.getpSendMilestone() ? milestoneTracker.latestMilestone : tipsViewModel.getRandomSolidTipHash();

        return tip == null ? Hash.NULL_HASH : tip;
    }
}
