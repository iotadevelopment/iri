package com.iota.iri.service.ledger.impl;

import com.iota.iri.BundleValidator;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.ledger.LedgerService;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.service.snapshot.impl.SnapshotStateDiffImpl;
import com.iota.iri.storage.Tangle;
import com.iota.iri.zmq.MessageQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LedgerServiceImpl implements LedgerService {
    private final Logger log = LoggerFactory.getLogger(LedgerServiceImpl.class);

    private final MilestoneService milestoneService;

    private final SnapshotService snapshotService;

    public LedgerServiceImpl(SnapshotService snapshotService, MilestoneService milestoneService) {
        this.snapshotService = snapshotService;
        this.milestoneService = milestoneService;
    }

    public boolean applyMilestoneToLedger(Tangle tangle, SnapshotProvider snapshotProvider, MessageQ messageQ, MilestoneViewModel milestone) throws Exception {
        if(generateStateDiff(tangle, snapshotProvider, messageQ, milestone)) {
            snapshotService.replayMilestones(tangle, snapshotProvider.getLatestSnapshot(), milestone.index());

            return true;
        }

        return false;
    }

    public boolean checkTipConsistency(Tangle tangle, SnapshotProvider snapshotProvider, List<Hash> hashes) throws Exception {
        Set<Hash> visitedHashes = new HashSet<>();
        Map<Hash, Long> diff = new HashMap<>();
        for (Hash hash : hashes) {
            if (!isBalanceDiffConsistent(tangle, snapshotProvider, visitedHashes, diff, hash)) {
                return false;
            }
        }
        return true;
    }

    public Map<Hash,Long> generateBalanceDiff(Tangle tangle, SnapshotProvider snapshotProvider, Set<Hash> visitedNonMilestoneSubtangleHashes, Hash tip, int latestSnapshotIndex, boolean milestone) throws Exception {
        Map<Hash, Long> state = new HashMap<>();
        int numberOfAnalyzedTransactions = 0;
        Set<Hash> countedTx = new HashSet<>();

        snapshotProvider.getInitialSnapshot().getSolidEntryPoints().keySet().forEach(solidEntryPointHash -> {
            visitedNonMilestoneSubtangleHashes.add(solidEntryPointHash);
            countedTx.add(solidEntryPointHash);
        });

        final Queue<Hash> nonAnalyzedTransactions = new LinkedList<>(Collections.singleton(tip));
        Hash transactionPointer;
        while ((transactionPointer = nonAnalyzedTransactions.poll()) != null) {
            if (visitedNonMilestoneSubtangleHashes.add(transactionPointer)) {

                final TransactionViewModel transactionViewModel = TransactionViewModel.fromHash(tangle, transactionPointer);
                if (transactionViewModel.snapshotIndex() == 0 || transactionViewModel.snapshotIndex() > latestSnapshotIndex) {
                    numberOfAnalyzedTransactions++;
                    if (transactionViewModel.getType() == TransactionViewModel.PREFILLED_SLOT) {
                        // DONT SOLIDIFY HERE ? transactionRequester.requestTransaction(transactionViewModel.getHash(), milestone);
                        return null;

                    } else {

                        if (transactionViewModel.getCurrentIndex() == 0) {

                            boolean validBundle = false;

                            final List<List<TransactionViewModel>> bundleTransactions = BundleValidator.validate(tangle, snapshotProvider.getInitialSnapshot(), transactionViewModel.getHash());
                            /*
                            for(List<TransactionViewModel> transactions: bundleTransactions) {
                                if (transactions.size() > 0) {
                                    int index = transactions.get(0).snapshotIndex();
                                    if (index > 0 && index <= latestSnapshotIndex) {
                                        return null;
                                    }
                                }
                            }
                            */
                            for (final List<TransactionViewModel> bundleTransactionViewModels : bundleTransactions) {

                                if(BundleValidator.isInconsistent(bundleTransactionViewModels)) {
                                    break;
                                }
                                if (bundleTransactionViewModels.get(0).getHash().equals(transactionViewModel.getHash())) {

                                    validBundle = true;

                                    for (final TransactionViewModel bundleTransactionViewModel : bundleTransactionViewModels) {

                                        if (bundleTransactionViewModel.value() != 0 && countedTx.add(bundleTransactionViewModel.getHash())) {

                                            final Hash address = bundleTransactionViewModel.getAddressHash();
                                            final Long value = state.get(address);
                                            state.put(address, value == null ? bundleTransactionViewModel.value()
                                                    : Math.addExact(value, bundleTransactionViewModel.value()));
                                        }
                                    }

                                    break;
                                }
                            }
                            if (!validBundle) {
                                return null;
                            }
                        }

                        nonAnalyzedTransactions.offer(transactionViewModel.getTrunkTransactionHash());
                        nonAnalyzedTransactions.offer(transactionViewModel.getBranchTransactionHash());
                    }
                }
            }
        }

        log.debug("Analyzed transactions = " + numberOfAnalyzedTransactions);

        return state;
    }

    public boolean generateStateDiff(Tangle tangle, SnapshotProvider snapshotProvider, MessageQ messageQ, MilestoneViewModel milestoneVM) throws Exception {
        TransactionViewModel transactionViewModel = TransactionViewModel.fromHash(tangle, milestoneVM.getHash());

        if(!transactionViewModel.isSolid()) {
            return false;
        }

        final int transactionSnapshotIndex = transactionViewModel.snapshotIndex();
        boolean successfullyProcessed = transactionSnapshotIndex == milestoneVM.index();
        if (!successfullyProcessed) {
            // if the snapshotIndex of our transaction was set already, we have processed our milestones in
            // the wrong order (i.e. while rescanning the db)
            if(transactionSnapshotIndex != 0) {
                milestoneService.resetCorruptedMilestone(tangle, snapshotProvider, messageQ, milestoneVM.index(), "generateStateDiff");
            }

            snapshotProvider.getLatestSnapshot().lockRead();
            try {
                Hash tail = transactionViewModel.getHash();
                Map<Hash, Long> balanceChanges = generateBalanceDiff(tangle, snapshotProvider, new HashSet<>(), tail, snapshotProvider.getLatestSnapshot().getIndex(), true);
                successfullyProcessed = balanceChanges != null;
                if(successfullyProcessed) {
                    successfullyProcessed = snapshotProvider.getLatestSnapshot().patchedState(new SnapshotStateDiffImpl(balanceChanges)).isConsistent();
                    if(successfullyProcessed) {
                        milestoneService.updateMilestoneIndexOfMilestoneTransactions(tangle, snapshotProvider, messageQ, milestoneVM.getHash(), milestoneVM.index());

                        if(balanceChanges.size() != 0) {
                            new StateDiffViewModel(balanceChanges, milestoneVM.getHash()).store(tangle);
                        }
                    }
                }
            } finally {
                snapshotProvider.getLatestSnapshot().unlockRead();
            }
        }

        return successfullyProcessed;
    }

    public boolean isBalanceDiffConsistent(Tangle tangle, SnapshotProvider snapshotProvider, Set<Hash> approvedHashes, final Map<Hash, Long> diff, Hash tip) throws Exception {
        if(!TransactionViewModel.fromHash(tangle, tip).isSolid()) {
            return false;
        }
        if (approvedHashes.contains(tip)) {
            return true;
        }
        Set<Hash> visitedHashes = new HashSet<>(approvedHashes);
        Map<Hash, Long> currentState = generateBalanceDiff(tangle, snapshotProvider, visitedHashes, tip, snapshotProvider.getLatestSnapshot().getIndex(), false);
        if (currentState == null) {
            return false;
        }
        diff.forEach((key, value) -> {
            if(currentState.computeIfPresent(key, ((hash, aLong) -> value + aLong)) == null) {
                currentState.putIfAbsent(key, value);
            }
        });
        boolean isConsistent = snapshotProvider.getLatestSnapshot().patchedState(new SnapshotStateDiffImpl(currentState)).isConsistent();
        if (isConsistent) {
            diff.putAll(currentState);
            approvedHashes.addAll(visitedHashes);
        }
        return isConsistent;
    }
}
