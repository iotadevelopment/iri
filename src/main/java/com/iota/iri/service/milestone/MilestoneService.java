package com.iota.iri.service.milestone;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.crypto.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.zmq.MessageQ;

/**
 * Represents the service that contains all the relevant business logic for interacting with milestones.<br />
 * <br />
 * This class is stateless and does not hold any domain specific models.<br />
 */
public interface MilestoneService {
    /**
     * This method analyzes the given transaction to determine if it is a valid milestone.<br />
     * <br />
     * It first checks if all transactions that belong to the milestone bundle are known already and only then verifies
     * the signature to analyze if the given milestone was really issued by the coordinator.<br />
     *
     * @param tangle Tangle object which acts as a database interface [dependency]
     * @param snapshotProvider snapshot provider which gives us access to the relevant snapshots [dependency]
     * @param messageQ ZeroMQ interface that allows us to emit messages for external recipients [dependency]
     * @param config config with important milestone specific settings [dependency]
     * @param transactionViewModel transaction that shall be analyzed
     * @param mode mode that gets used for the signature verification
     * @param securityLevel security level that gets used for the signature verification
     * @return validity status of the transaction regarding its role as a milestone
     * @throws MilestoneException if anything unexpected goes wrong while validating the milestone transaction
     */
    MilestoneValidity validateMilestone(Tangle tangle, SnapshotProvider snapshotProvider, MessageQ messageQ,
            IotaConfig config, TransactionViewModel transactionViewModel, SpongeFactory.Mode mode, int securityLevel)
            throws MilestoneException;

    /**
     * This method updates the milestone index of all transactions that belong to a milestone.<br />
     * <br />
     * It does that by iterating through all approvees of the milestone defined by the given {@code milestoneHash} until
     * it reaches transactions that have been approved by a previous milestone. This means that this method only works
     * if the transactions belonging to the previous milestone have been updated already.<br />
     * <br />
     * While iterating through the transactions we also examine the milestone index that is currently set to detect
     * corruptions in the database where a following milestone was processed before the current one. If such a
     * corruption in the database is found we trigger a reset of the wrongly processed milestone to repair the ledger
     * state and recover from this error.<br />
     * <br />
     * In addition to these checks we also update the solid entry points, if we detect that a transaction references a
     * transaction that dates back before the last local snapshot (and that has not been marked as a solid entry point,
     * yet). This allows us to handle back-referencing transactions and maintain local snapshot files that can always be
     * used to bootstrap a node, even if the coordinator suddenly approves really old transactions (this only works
     * if the transaction that got referenced is still "known" to the node by having a sufficiently high pruning
     * delay).<br />
     *
     * @param tangle Tangle object which acts as a database interface [dependency]
     * @param snapshotProvider snapshot provider which gives us access to the relevant snapshots [dependency]
     * @param messageQ ZeroMQ interface that allows us to emit messages for external recipients [dependency]
     * @param milestoneHash the hash of the transaction
     * @param newIndex the milestone index that shall be set
     * @throws MilestoneException if anything unexpected happens while updating the milestone index
     */
    void updateMilestoneIndexOfMilestoneTransactions(Tangle tangle, SnapshotProvider snapshotProvider,
            MessageQ messageQ, Hash milestoneHash, int newIndex) throws MilestoneException;

    /**
     * This method resets all milestone related information of the transactions that were "confirmed" by the given
     * milestone and rolls back the ledger state to the moment before the milestone was applied.<br />
     * <br />
     * This allows us to reprocess the milestone in case of errors where the given milestone could not be applied to the
     * ledger state. It is for example used by the automatic repair routine of the {@link LatestSolidMilestoneTracker}
     * (to recover from inconsistencies due to crashes of IRI).<br />
     * <br />
     * It recursively resets additional milestones if inconsistencies are found within the resetted milestone (wrong
     * {@code milestoneIndex}es).<br />
     *
     * @param tangle Tangle object which acts as a database interface [dependency]
     * @param snapshotProvider snapshot provider which gives us access to the relevant snapshots [dependency]
     * @param messageQ ZeroMQ interface that allows us to emit messages for external recipients [dependency]
     * @param milestoneIndex milestone index that shall
     * @throws MilestoneException if anything goes wrong while resetting the corrupted milestone
     */
    void resetCorruptedMilestone(Tangle tangle, SnapshotProvider snapshotProvider, MessageQ messageQ,
            int milestoneIndex) throws MilestoneException;

    /**
     * This method checks if the given transaction "belongs" to the milestone with the given index.<br />
     * <br />
     * We determine if a transaction still belongs to the milestone with the given index by examining its
     * {@code snapshotIndex} value. For this method to work we require that the previous milestones have been processed
     * already (which is enforce by the {@link com.iota.iri.service.milestone.LatestSolidMilestoneTracker} which applies
     * the milestones in the order that they are issued by the coordinator).<br />
     *
     * @param transaction the transaction that shall be examined
     * @param milestoneIndex the milestone index that we want to check against
     * @return {@code true} if the transaction belongs to the milestone and {@code false} otherwise
     */
    boolean transactionBelongsToMilestone(TransactionViewModel transaction, int milestoneIndex);

    /**
     * This method retrieves the milestone index of the given transaction by decoding the {@code OBSOLETE_TAG}.<br />
     * <br />
     * The returned result will of cause only have a reasonable value if we hand in a transaction that represents a real
     * milestone.<br />
     *
     * @param milestoneTransaction the transaction that shall have its milestone index retrieved
     * @return the milestone index of the transaction
     */
    int getMilestoneIndex(TransactionViewModel milestoneTransaction);
}
