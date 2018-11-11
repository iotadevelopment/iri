package com.iota.iri.service.milestone.impl;

import com.iota.iri.BundleValidator;
import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hash.Curl;
import com.iota.iri.hash.ISS;
import com.iota.iri.hash.ISSInPlace;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.StateDiff;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.milestone.MilestoneValidity;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Converter;
import com.iota.iri.utils.dag.DAGHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.iota.iri.controllers.TransactionViewModel.OBSOLETE_TAG_TRINARY_OFFSET;
import static com.iota.iri.service.milestone.MilestoneValidity.INCOMPLETE;
import static com.iota.iri.service.milestone.MilestoneValidity.INVALID;
import static com.iota.iri.service.milestone.MilestoneValidity.VALID;

public class MilestoneServiceImpl implements MilestoneService {
    private final static Logger log = LoggerFactory.getLogger(MilestoneServiceImpl.class);

    @Override
    public MilestoneValidity validateMilestone(Tangle tangle, SnapshotProvider snapshotProvider,
            SnapshotService snapshotService, IotaConfig config, TransactionViewModel transactionViewModel, SpongeFactory.Mode mode,
            int securityLevel) throws Exception {

        int milestoneIndex = getMilestoneIndex(transactionViewModel);
        if (milestoneIndex < 0 || milestoneIndex >= 0x200000) {
            return INVALID;
        }

        if (MilestoneViewModel.get(tangle, milestoneIndex) != null) {
            // Already validated.
            return VALID;
        }
        final List<List<TransactionViewModel>> bundleTransactions = BundleValidator.validate(tangle, snapshotProvider.getInitialSnapshot(), transactionViewModel.getHash());
        if (bundleTransactions.size() == 0) {
            return INCOMPLETE;
        }
        else {
            for (final List<TransactionViewModel> bundleTransactionViewModels : bundleTransactions) {

                final TransactionViewModel tail = bundleTransactionViewModels.get(0);
                if (tail.getHash().equals(transactionViewModel.getHash())) {
                    //the signed transaction - which references the confirmed transactions and contains
                    // the Merkle tree siblings.
                    final TransactionViewModel siblingsTx = bundleTransactionViewModels.get(securityLevel);

                    if (isMilestoneBundleStructureValid(bundleTransactionViewModels, securityLevel)) {
                        //milestones sign the normalized hash of the sibling transaction.
                        byte[] signedHash = ISS.normalizedBundle(siblingsTx.getHash().trits());

                        //validate leaf signature
                        ByteBuffer bb = ByteBuffer.allocate(Curl.HASH_LENGTH * securityLevel);
                        byte[] digest = new byte[Curl.HASH_LENGTH];

                        for (int i = 0; i < securityLevel; i++) {
                            ISSInPlace.digest(mode, signedHash, ISS.NUMBER_OF_FRAGMENT_CHUNKS * i,
                                    bundleTransactionViewModels.get(i).getSignature(), 0, digest);
                            bb.put(digest);
                        }

                        byte[] digests = bb.array();
                        byte[] address = ISS.address(mode, digests);

                        //validate Merkle path
                        byte[] merkleRoot = ISS.getMerkleRoot(mode, address,
                                siblingsTx.trits(), 0, milestoneIndex, config.getNumberOfKeysInMilestone());
                        if ((config.isTestnet() && config.isDontValidateTestnetMilestoneSig()) || (HashFactory.ADDRESS.create(merkleRoot)).equals(HashFactory.ADDRESS.create(config.getCoordinator()))) {
                            MilestoneViewModel newMilestoneViewModel = new MilestoneViewModel(milestoneIndex, transactionViewModel.getHash());
                            newMilestoneViewModel.store(tangle);

                            // if we find a NEW milestone that should have been processed before our latest solid
                            // milestone -> reset the ledger state and check the milestones again
                            //
                            // NOTE: this can happen if a new subtangle becomes solid before a previous one while syncing
                            if(milestoneIndex < snapshotProvider.getLatestSnapshot().getIndex() && milestoneIndex > snapshotProvider.getInitialSnapshot().getIndex()) {
                                try {
                                    snapshotService.rollBackMilestones(tangle, snapshotProvider.getLatestSnapshot(), newMilestoneViewModel.index());
                                } catch(SnapshotException e) {
                                    log.error("could not reset ledger to missing milestone: " + milestoneIndex);
                                }
                            }
                            return VALID;
                        } else {
                            return INVALID;
                        }
                    }
                }
            }
        }
        return INVALID;
    }

    @Override
    public void resetCorruptedMilestone(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotService snapshotService, int milestoneIndex, String identifier) {
        resetCorruptedMilestone(tangle, snapshotProvider, snapshotService, milestoneIndex, identifier, new HashSet<>());
    }

    @Override
    public int getMilestoneIndex(TransactionViewModel milestoneTransaction) {
        return (int) Converter.longValue(milestoneTransaction.trits(), OBSOLETE_TAG_TRINARY_OFFSET, 15);
    }

    //trunks of bundles are checked in Bundle validation - no need to check again.
    // bundleHash equality is checked in BundleValidator.validate() (loadTransactionsFromTangle)
    @Override
    public boolean isMilestoneBundleStructureValid(List<TransactionViewModel> bundleTransactions, int securityLevel) {
        if (bundleTransactions.size() <= securityLevel) {
            return false;
        }

        Hash headTransactionHash = bundleTransactions.get(securityLevel).getTrunkTransactionHash();
        return bundleTransactions.stream()
                .limit(securityLevel)
                .map(TransactionViewModel::getBranchTransactionHash)
                .allMatch(branchTransactionHash -> branchTransactionHash.equals(headTransactionHash));
    }

    private void resetCorruptedMilestone(Tangle tangle, SnapshotProvider snapshotProvider,
            SnapshotService snapshotService, int milestoneIndex, String identifier,
            HashSet<Hash> processedTransactions) {

        if(milestoneIndex <= snapshotProvider.getInitialSnapshot().getIndex()) {
            return;
        }

        System.out.println("REPAIRING: " + snapshotProvider.getLatestSnapshot().getIndex() + " <=> " + milestoneIndex + " => " + identifier);

        try {
            MilestoneViewModel milestoneToRepair = MilestoneViewModel.get(tangle, milestoneIndex);

            if(milestoneToRepair != null) {
                // reset the ledger to the state before the erroneous milestone appeared
                if(milestoneToRepair.index() <= snapshotProvider.getLatestSnapshot().getIndex()) {
                    snapshotService.rollBackMilestones(tangle, snapshotProvider.getLatestSnapshot(),
                            milestoneToRepair.index());
                }

                resetMilestoneIndexOfMilestoneTransactions(tangle, snapshotProvider, snapshotService, milestoneToRepair, processedTransactions);
                tangle.delete(StateDiff.class, milestoneToRepair.getHash());
            }
        } catch (Exception e) {
            log.error("failed to repair corrupted milestone with index #" + milestoneIndex, e);
        }
    }

    /**
     * This method resets the snapshotIndex of all transactions that "belong" to a milestone.
     *
     * While traversing the graph we use the snapshotIndex value to check if it still belongs to the given milestone and
     * ignore all transactions that where referenced by a previous ones. Since we check if the snapshotIndex is bigger
     * or equal to the one of the targetMilestone, we can ignore the case that a previous milestone was not processed,
     * yet since it's milestoneIndex would still be 0.
     *
     * @param currentMilestone the milestone that shall have its confirmed transactions reset
     * @throws Exception if something goes wrong while accessing the database
     */
    public void resetMilestoneIndexOfMilestoneTransactions(Tangle tangle, SnapshotProvider snapshotProvider,
            SnapshotService snapshotService, MilestoneViewModel currentMilestone, HashSet<Hash> processedTransactions) {

        try {
            Set<Integer> resettedMilestones = new HashSet<>();

            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(tangle, currentMilestone.getHash());
            resetMilestoneIndexOfSingleTransaction(tangle, snapshotProvider, milestoneTransaction, currentMilestone, resettedMilestones);
            processedTransactions.add(milestoneTransaction.getHash());

            DAGHelper.get(tangle).traverseApprovees(
                currentMilestone.getHash(),
                currentTransaction -> currentTransaction.snapshotIndex() >= currentMilestone.index() || currentTransaction.snapshotIndex() == 0,
                currentTransaction -> resetMilestoneIndexOfSingleTransaction(tangle, snapshotProvider, currentTransaction, currentMilestone, resettedMilestones),
                processedTransactions
            );

            for(int resettedMilestoneIndex : resettedMilestones) {
                resetCorruptedMilestone(tangle, snapshotProvider, snapshotService, resettedMilestoneIndex, "resetMilestoneIndexOfMilestoneTransactions", processedTransactions);
            }
        } catch(Exception e) {
            log.error("failed to reset the transactions belonging to " + currentMilestone, e);
        }
    }

    private void resetMilestoneIndexOfSingleTransaction(Tangle tangle, SnapshotProvider snapshotProvider,
            TransactionViewModel currentTransaction, MilestoneViewModel currentMilestone,
            Set<Integer> resettedMilestones) {

        try {
            if(currentTransaction.snapshotIndex() > currentMilestone.index()) {
                resettedMilestones.add(currentTransaction.snapshotIndex());
            }

            currentTransaction.setSnapshot(tangle, snapshotProvider.getInitialSnapshot(), 0);
        } catch(Exception e) {
            log.error("failed to reset the snapshotIndex of " + currentTransaction + " while trying to repair " + currentMilestone, e);
        }
    }
}
