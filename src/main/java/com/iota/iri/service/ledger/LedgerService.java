package com.iota.iri.service.ledger;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.zmq.MessageQ;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface LedgerService {
    boolean applyMilestoneToLedger(Tangle tangle, SnapshotProvider snapshotProvider, MessageQ messageQ, MilestoneViewModel milestone) throws Exception;

    boolean checkTipConsistency(Tangle tangle, SnapshotProvider snapshotProvider, List<Hash> hashes) throws Exception;

    Map<Hash,Long> generateBalanceDiff(Tangle tangle, SnapshotProvider snapshotProvider, Set<Hash> visitedNonMilestoneSubtangleHashes, Hash tip, int latestSnapshotIndex, boolean milestone) throws Exception;

    boolean generateStateDiff(Tangle tangle, SnapshotProvider snapshotProvider, MessageQ messageQ, MilestoneViewModel milestoneVM) throws Exception;

    boolean isBalanceDiffConsistent(Tangle tangle, SnapshotProvider snapshotProvider, Set<Hash> approvedHashes, final Map<Hash, Long> diff, Hash tip) throws Exception;
}
