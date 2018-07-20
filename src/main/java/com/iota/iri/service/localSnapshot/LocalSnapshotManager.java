package com.iota.iri.service.localSnapshot;

import com.iota.iri.Iota;
import com.iota.iri.Milestone;
import com.iota.iri.Snapshot;
import com.iota.iri.controllers.ApproveeViewModel;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;

import java.util.*;
import java.util.stream.Collectors;

public class LocalSnapshotManager {
    Iota instance;

    public LocalSnapshotManager(Iota instance) {
        // store a reference to the IOTA instance so we can access all relevant objects
        this.instance = instance;
    }

    public Snapshot getSnapshot(Hash milestoneHash) throws Exception {
        return getSnapshot(MilestoneViewModel.fromHash(instance.tangle, milestoneHash));
    }

    public Snapshot getSnapshot(int milestoneIndex) throws Exception {
        return getSnapshot(MilestoneViewModel.get(instance.tangle, milestoneIndex));
    }

    public Snapshot getSnapshot(MilestoneViewModel targetMilestone) throws Exception {
        // check if the milestone was solidified already
        if(targetMilestone.index() > instance.milestone.latestSolidSubtangleMilestoneIndex) {
            throw new IllegalArgumentException("milestone not solidified yet");
        }

        // clone the current snapshot state
        Snapshot snapshot = instance.milestone.latestSnapshot.clone();

        // if the target is the latest milestone we can return immediately
        if(targetMilestone.index() == instance.milestone.latestSolidSubtangleMilestoneIndex) {
            return snapshot;
        }

        // retrieve the latest milestone
        MilestoneViewModel currentMilestone = MilestoneViewModel.get(instance.tangle, instance.milestone.latestSolidSubtangleMilestoneIndex);

        // descend the milestones down to our target
        while(currentMilestone.index() > targetMilestone.index()) {
            // apply the balance changes to the snapshot (with inverted values)
            snapshot.apply(
                StateDiffViewModel.load(instance.tangle, currentMilestone.getHash()).getDiff().entrySet().stream().map(
                    hashLongEntry -> new HashMap.SimpleEntry<>(hashLongEntry.getKey(), -hashLongEntry.getValue())
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                currentMilestone.index()
            );

            // iterate to the next milestone
            currentMilestone = currentMilestone.previous(instance.tangle);
        }

        // return the result
        return snapshot;
    }

    public boolean getTransactionsToPrune(Hash milestoneHash) throws Exception {
        return getTransactionsToPrune(MilestoneViewModel.fromHash(instance.tangle, milestoneHash));
    }

    public boolean getTransactionsToPrune(int milestoneIndex) throws Exception {
        return getTransactionsToPrune(MilestoneViewModel.get(instance.tangle, milestoneIndex));
    }

    public boolean getTransactionsToPrune(MilestoneViewModel targetMilestone) throws Exception {
        int transactionCount = 0;

        // iterate down through the tangle in "steps" (one milestone at a time) so the data structures don't get too big
        MilestoneViewModel currentMilestone = targetMilestone;
        while(currentMilestone != null) {
            // retrieve the transaction belonging to our current milestone
            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(
                instance.tangle,
                currentMilestone.getHash()
            );

            // create a queue where we collect the transactions that shall be deleted (starting with our milestone)
            final Queue<TransactionViewModel> transactionsToDelete = new LinkedList<>(
                Collections.singleton(milestoneTransaction)
            );

            // create a set of visited transactions to prevent processing the same transaction more than once
            Set<Hash> visitedTransactions = new HashSet<>(Collections.singleton(Hash.NULL_HASH));

            // create a set of visited transactions to prevent processing the same transaction more than once
            Set<Hash> visitedParents = new HashSet<>(Collections.singleton(Hash.NULL_HASH));

            // ITERATE DOWN TOWARDS THE GENESIS ////////////////////////////////////////////////////////////////////////

            // iterate through our queue and process all elements (while we iterate we add more)
            TransactionViewModel currentTransaction;
            while((currentTransaction = transactionsToDelete.poll()) != null) {
                // check if we see this transaction the first time
                if(visitedTransactions.add(currentTransaction.getHash())) {
                    // retrieve the two referenced transactions
                    TransactionViewModel branchTransaction = currentTransaction.getBranchTransaction(instance.tangle);
                    TransactionViewModel trunkTransaction = currentTransaction.getTrunkTransaction(instance.tangle);

                    // if the transaction was approved by our current milestone (part of the current step) -> queue it
                    if(branchTransaction.snapshotIndex() == currentMilestone.index()) {
                        transactionsToDelete.offer(branchTransaction);
                    }

                    // if the transaction was approved by our current milestone (part of the current step) -> queue it
                    if(trunkTransaction.snapshotIndex() == currentMilestone.index()) {
                        transactionsToDelete.offer(trunkTransaction);
                    }

                    // check if we have "children" who didn't get approved yet and only reference a milestone below our current depth
                    ApproveeViewModel approvers = currentTransaction.getApprovers(instance.tangle);

                    for(Hash approverHash: approvers.getHashes()) {
                        if(!visitedTransactions.contains(approverHash)) {
                            TransactionViewModel approverTransaction = TransactionViewModel.fromHash(instance.tangle, approverHash);
                            if(approverTransaction.snapshotIndex() == 0) {
                                transactionCount++;
                            }
                        }
                    }

                    transactionCount++;
                }

                // output hash (for debugging)
                if(transactionCount % 10000 == 0) {
                    System.out.println("DELETED: " + transactionCount + " / " + currentMilestone.index());
                }
            }

            // go to the next milestone chunk
            currentMilestone = currentMilestone.previous(instance.tangle);
        }

        // output hash (for debugging)
        System.out.println("DELETED: " + transactionCount);

        return true;
    }
}
