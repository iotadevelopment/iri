package com.iota.iri.service.milestone.impl;

import com.iota.iri.BundleValidator;
import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.crypto.Curl;
import com.iota.iri.crypto.ISS;
import com.iota.iri.crypto.ISSInPlace;
import com.iota.iri.crypto.SpongeFactory;
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

/**
 * Implements the basic contract of the {@link MilestoneService}.
 */
public class MilestoneServiceImpl implements MilestoneService {
    /**
     * Holds the logger of this class (gets used to issue error messages).<br />
     */
    private final static Logger log = LoggerFactory.getLogger(MilestoneServiceImpl.class);

    /**
     * Holds a reference to the service instance of the snapshot package that allows us to rollback ledger states.<br />
     */
    private final SnapshotService snapshotService;

    /**
     * Creates a service instance that allows us to interact with the milestones.<br />
     * <br />
     * It simply stores the passed in dependencies in the internal properties.<br />
     *
     * @param snapshotService service instance of the snapshot package that allows us to rollback ledger states
     */
    public MilestoneServiceImpl(SnapshotService snapshotService) {
        this.snapshotService = snapshotService;
    }

    @Override
    public MilestoneValidity validateMilestone(Tangle tangle, SnapshotProvider snapshotProvider, IotaConfig config,
            TransactionViewModel transactionViewModel, SpongeFactory.Mode mode, int securityLevel) throws Exception {

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
        } else {
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

    /**
     * {@inheritDoc}
     * <br />
     * We redirect the call to {@link #resetCorruptedMilestone(Tangle, SnapshotProvider, int, String, HashSet)} while
     * initiating the set of {@code processedTransactions} with an empty {@link HashSet} which will ensure that we reset
     * all found transactions.<br />
     */
    @Override
    public void resetCorruptedMilestone(Tangle tangle, SnapshotProvider snapshotProvider, int milestoneIndex,
            String identifier) {

        resetCorruptedMilestone(tangle, snapshotProvider, milestoneIndex, identifier, new HashSet<>());
    }

    @Override
    public int getMilestoneIndex(TransactionViewModel milestoneTransaction) {
        return (int) Converter.longValue(milestoneTransaction.trits(), OBSOLETE_TAG_TRINARY_OFFSET, 15);
    }

    /**
     * This method is a utility method that checks if the transactions belonging to the potential milestone bundle have
     * a valid structure (used during the validation of milestones).<br />
     * <br />
     * It first checks if the bundle has enough transactions to conform to the given {@code securityLevel} and then
     * verifies that the {@code branchTransactionsHash}es are pointing to the {@code trunkTransactionHash} of the head
     * transactions.<br />
     *
     * @param bundleTransactions all transactions belonging to the milestone
     * @param securityLevel the security level used for the signature
     * @return {@code true} if the basic structure is valid and {@code false} otherwise
     */
    private boolean isMilestoneBundleStructureValid(List<TransactionViewModel> bundleTransactions, int securityLevel) {
        if (bundleTransactions.size() <= securityLevel) {
            return false;
        }

        Hash headTransactionHash = bundleTransactions.get(securityLevel).getTrunkTransactionHash();
        return bundleTransactions.stream()
                .limit(securityLevel)
                .map(TransactionViewModel::getBranchTransactionHash)
                .allMatch(branchTransactionHash -> branchTransactionHash.equals(headTransactionHash));
    }

    /**
     * This method does the same as {@link #resetCorruptedMilestone(Tangle, SnapshotProvider, int, String)} but
     * additionally receives a set of {@code processedTransactions} that will allow us to not process the same
     * transactions over and over again while resetting additional milestones in recursive calls.<br />
     * <br />
     * It first checks if the desired {@code milestoneIndex} is reachable by this node and then triggers the
     * reset by:<br />
     * <br />
     * 1. resetting the ledger state if it addresses a milestone before the current latest solid milestone<br />
     * 2. resetting the {@code milestoneIndex} of all transactions that were confirmed by the current milestone<br />
     * 3. deleting the corresponding {@link StateDiff} entry from the database
     *
     * @param tangle Tangle object which acts as a database interface [dependency]
     * @param snapshotProvider snapshot provider which gives us access to the relevant snapshots [dependency]
     * @param milestoneIndex milestone index that shall
     * @param identifier string identifier for debug messages
     * @param processedTransactions a set of transactions that have been processed already
     */
    private void resetCorruptedMilestone(Tangle tangle, SnapshotProvider snapshotProvider, int milestoneIndex,
            String identifier, HashSet<Hash> processedTransactions) {

        if(milestoneIndex <= snapshotProvider.getInitialSnapshot().getIndex()) {
            return;
        }

        log.info("resetting corrupted milestone #" + milestoneIndex + " (source: " + identifier + ")");

        try {
            MilestoneViewModel milestoneToRepair = MilestoneViewModel.get(tangle, milestoneIndex);
            if(milestoneToRepair != null) {
                if(milestoneToRepair.index() <= snapshotProvider.getLatestSnapshot().getIndex()) {
                    snapshotService.rollBackMilestones(tangle, snapshotProvider.getLatestSnapshot(),
                            milestoneToRepair.index());
                }

                resetMilestoneIndexOfMilestoneTransactions(tangle, snapshotProvider, milestoneToRepair, processedTransactions);
                tangle.delete(StateDiff.class, milestoneToRepair.getHash());
            }
        } catch (Exception e) {
            log.error("failed to repair corrupted milestone with index #" + milestoneIndex, e);
        }
    }

    /**
     * This method resets the {@code milestoneIndex} of all transactions that "belong" to a milestone.<br />
     * <br />
     * While traversing the graph we use the {@code milestoneIndex} value to check if it still belongs to the given
     * milestone and ignore all transactions that were referenced by a previous one. Since we check if the
     * {@code milestoneIndex} is bigger or equal to the one of the target milestone, we can ignore the case that a
     * previous milestone was not processed, yet since it's milestoneIndex would still be 0.<br />
     * <br />
     * If we found inconsistencies in the {@code milestoneIndex} we recursively reset the milestones that referenced
     * them.<br />
     *
     * @param tangle Tangle object which acts as a database interface [dependency]
     * @param snapshotProvider snapshot provider which gives us access to the relevant snapshots [dependency]
     * @param targetMilestone the milestone that shall have his transactions reset
     * @param processedTransactions a set of transactions that have been processed already
     */
    private void resetMilestoneIndexOfMilestoneTransactions(Tangle tangle, SnapshotProvider snapshotProvider,
            MilestoneViewModel targetMilestone, HashSet<Hash> processedTransactions) {

        try {
            Set<Integer> inconsistentMilestones = new HashSet<>();

            TransactionViewModel milestoneTx = TransactionViewModel.fromHash(tangle, targetMilestone.getHash());
            processedTransactions.add(milestoneTx.getHash());
            resetMilestoneIndexOfSingleTransaction(tangle, snapshotProvider, milestoneTx, targetMilestone,
                    inconsistentMilestones);

            DAGHelper.get(tangle).traverseApprovees(
                targetMilestone.getHash(),
                currentTransaction -> currentTransaction.snapshotIndex() >= targetMilestone.index() ||
                                              currentTransaction.snapshotIndex() == 0,
                currentTransaction -> resetMilestoneIndexOfSingleTransaction(tangle, snapshotProvider,
                                              currentTransaction, targetMilestone, inconsistentMilestones),
                processedTransactions
            );

            for(int resettedMilestoneIndex : inconsistentMilestones) {
                resetCorruptedMilestone(tangle, snapshotProvider, resettedMilestoneIndex,
                        "resetMilestoneIndexOfMilestoneTransactions", processedTransactions);
            }
        } catch(Exception e) {
            log.error("failed to reset the transactions belonging to " + targetMilestone, e);
        }
    }

    /**
     * This method resets the {@code milestoneIndex} of a single transaction.<br />
     * <br />
     * If the {@code milestoneIndex} is bigger than the expected one, we collect the {@code milestoneIndex} so the
     * milestone wrongly referencing this transaction can recursively be repaired as well (if milestones got processed
     * in the wrong order).<br />
     *
     * @param tangle Tangle object which acts as a database interface [dependency]
     * @param snapshotProvider snapshot provider which gives us access to the relevant snapshots [dependency]
     * @param currentTransaction the transaction that shall have its {@code milestoneIndex} reset
     * @param targetMilestone the milestone that this transaction belongs to
     * @param inconsistentMilestones a mutable object that is used to collect inconsistencies
     */
    private void resetMilestoneIndexOfSingleTransaction(Tangle tangle, SnapshotProvider snapshotProvider,
            TransactionViewModel currentTransaction, MilestoneViewModel targetMilestone,
            Set<Integer> inconsistentMilestones) {

        try {
            if(currentTransaction.snapshotIndex() > targetMilestone.index()) {
                inconsistentMilestones.add(currentTransaction.snapshotIndex());
            }

            currentTransaction.setSnapshot(tangle, snapshotProvider.getInitialSnapshot(), 0);
        } catch(Exception e) {
            log.error("failed to reset the snapshotIndex of " + currentTransaction + " while trying to repair " +
                    targetMilestone, e);
        }
    }
}
