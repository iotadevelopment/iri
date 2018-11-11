package com.iota.iri.service.milestone;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.storage.Tangle;

import java.util.List;

public interface MilestoneService {
    MilestoneValidity validateMilestone(Tangle tangle, SnapshotProvider snapshotProvider,
            SnapshotService snapshotService, IotaConfig config, TransactionViewModel transactionViewModel,
            SpongeFactory.Mode mode, int securityLevel) throws Exception;

    void resetCorruptedMilestone(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotService snapshotService,
            int milestoneIndex, String identifier);

    int getMilestoneIndex(TransactionViewModel milestoneTransaction);

    boolean isMilestoneBundleStructureValid(List<TransactionViewModel> bundleTransactions, int securityLevel);
}
