package com.iota.iri.service.milestone;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.crypto.SpongeFactory;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;

import java.util.List;

/**
 * Represents the service that contains all the relevant business logic for this package.<br />
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
     * @param config config with important milestone specific settings [dependency]
     * @param transactionViewModel transaction that shall be analyzed
     * @param mode mode that gets used for the signature verification
     * @param securityLevel security level that gets used for the signature verification
     * @return validity status of the transaction regarding its role as a milestone
     * @throws Exception if anything unexpected goes wrong while validating the milestone transaction
     */
    MilestoneValidity validateMilestone(Tangle tangle, SnapshotProvider snapshotProvider, IotaConfig config,
            TransactionViewModel transactionViewModel, SpongeFactory.Mode mode, int securityLevel) throws Exception;

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
     * @param milestoneIndex milestone index that shall
     * @param identifier string identifier for debug messages
     */
    void resetCorruptedMilestone(Tangle tangle, SnapshotProvider snapshotProvider, int milestoneIndex,
            String identifier);

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
