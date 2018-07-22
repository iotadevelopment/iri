package com.iota.iri.service.localSnapshot;

import com.iota.iri.Iota;
import com.iota.iri.Snapshot;
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

    int totalDeletedTransactions;

    int maxDeletedParentTransactions;

    int maxDeletedChildTransactions;

    int latestMilestoneIndex;

    int currentMilestoneIndex;

    int sideTangleTransactionsSeen;

    int sideTangleTransactionsDeleted;

    public int getReferencedMilestone(TransactionViewModel transaction) throws Exception {
        Hash branchTransactionHash = transaction.getBranchTransactionHash();
        Hash trunkTransactionHash = transaction.getTrunkTransactionHash();

        int referencedBranchTransactionMilestone;
        if(branchTransactionHash.equals(Hash.NULL_HASH)) {
            referencedBranchTransactionMilestone = 0;
        } else {
            try {
                referencedBranchTransactionMilestone = MilestoneViewModel.fromHash(instance.tangle, branchTransactionHash).index();
            } catch(Exception e) {
                referencedBranchTransactionMilestone = getReferencedMilestone(transaction.getBranchTransaction(instance.tangle));
            }
        }

        int referencedTrunkTransactionMilestone;
        if(branchTransactionHash.equals(Hash.NULL_HASH)) {
            referencedTrunkTransactionMilestone = 0;
        } else {
            try {
                referencedTrunkTransactionMilestone = MilestoneViewModel.fromHash(instance.tangle, branchTransactionHash).index();
            } catch(Exception e) {
                referencedTrunkTransactionMilestone = getReferencedMilestone(transaction.getTrunkTransaction(instance.tangle));
            }
        }

        return Math.max(referencedBranchTransactionMilestone, referencedTrunkTransactionMilestone);
    }

    public boolean getTransactionsToPrune(MilestoneViewModel targetMilestone) throws Exception {
        latestMilestoneIndex = instance.milestone.latestSolidSubtangleMilestoneIndex;
        totalDeletedTransactions = 0;
        maxDeletedParentTransactions = 0;
        maxDeletedChildTransactions = 0;
        sideTangleTransactionsSeen = 0;
        sideTangleTransactionsDeleted = 0;

        // create a set of visited transactions to prevent processing the same transaction more than once
        Set<Hash> visitedParents = new HashSet<>(Collections.singleton(Hash.NULL_HASH));

        // iterate down through the tangle in "steps" (one milestone at a time) so the data structures don't get too big
        MilestoneViewModel currentMilestone = targetMilestone;
        while(currentMilestone != null) {
            currentMilestoneIndex = currentMilestone.index();

            // retrieve the transaction belonging to our current milestone
            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(
                instance.tangle,
                currentMilestone.getHash()
            );

            //region ITERATE DOWN TOWARDS THE GENESIS //////////////////////////////////////////////////////////////////

            // create a queue where we collect the transactions that shall be deleted (starting with our milestone)
            final Queue<TransactionViewModel> transactionsToDelete = new LinkedList<>(
                Collections.singleton(milestoneTransaction)
            );

            // create a set of visited transactions to prevent processing the same transaction more than once
            Set<Hash> visitedTransactions = new HashSet<>(Collections.singleton(Hash.NULL_HASH));

            // create a set where we collect the candidates for orphaned parent transactions
            Set<Hash> possiblyOrphanedParents = new HashSet<>();

            //IOTASPAM9DOT9COM9999TYPPI99999999999999999999999999999999999999999999999999999999

            // gather some statistics
            int deletedChildTransactions = 0;

            // iterate through our queue and process all elements (while we iterate we add more)
            TransactionViewModel currentTransaction;
            while((currentTransaction = transactionsToDelete.poll()) != null) {
                if(currentTransaction.getHash().toString().equals("WTQYOYGUUDPCF9WYGLVJBL9IJIYRFNXCFEMYLSDRWAOYLQHQSKBWH9ENNXHNJGGX9TRBXCNAJCWZA9999")) {
                    sideTangleTransactionsSeen++;
                }

                // check if we see this transaction the first time
                if(visitedTransactions.add(currentTransaction.getHash())) {
                    if(currentTransaction.getHash().toString().equals("WTQYOYGUUDPCF9WYGLVJBL9IJIYRFNXCFEMYLSDRWAOYLQHQSKBWH9ENNXHNJGGX9TRBXCNAJCWZA9999")) {
                        sideTangleTransactionsDeleted++;
                    }

                    //System.out.println(currentTransaction.getTagValue().toString());

                    // retrieve the two referenced transactions
                    TransactionViewModel branchTransaction = currentTransaction.getBranchTransaction(instance.tangle);
                    TransactionViewModel trunkTransaction = currentTransaction.getTrunkTransaction(instance.tangle);

                    // if the transaction was approved by our current milestone (part of the current step) -> queue it
                    if(branchTransaction.snapshotIndex() == currentMilestone.index()) {
                        transactionsToDelete.add(branchTransaction);
                    }

                    // if the transaction was approved by our current milestone (part of the current step) -> queue it
                    if(trunkTransaction.snapshotIndex() == currentMilestone.index()) {
                        transactionsToDelete.add(trunkTransaction);
                    }

                    // if we have "parents" -> "remember" to check them in the next step if they are orphaned
                    for(Hash approverHash: currentTransaction.getApprovers(instance.tangle).getHashes()) {
                        // only add them if we didn't add them yet and if they are not part of the already cleaned ones
                        if(
                            !possiblyOrphanedParents.contains(approverHash) &&
                            !visitedTransactions.contains(approverHash)
                        ) {
                            possiblyOrphanedParents.add(approverHash);
                        }
                    }

                    // remove the current transaction from the orphaned parent candidates (processed it already)
                    possiblyOrphanedParents.remove(currentTransaction.getHash());

                    // TODO: ACTUALLY DELETE THE TRANSACTION

                    // increase our cleaned transactions counter (for debugging)
                    totalDeletedTransactions++;
                    deletedChildTransactions++;

                    // output statistics (for debugging)
                    dumpProgressStatistics();
                }
            }

            // gather some statistics
            maxDeletedChildTransactions = Math.max(maxDeletedChildTransactions, deletedChildTransactions);
            //endregion

            //region ITERATE UP TOWARDS THE TIPS (PARASITIC SIDE TANGLES) //////////////////////////////////////////////

            // create a queue where we collect the transactions that shall be deleted (starting with our milestone)
            final Queue<Hash> parentTransactionsToCheck = new LinkedList(possiblyOrphanedParents);

            // create a set of visited transactions to prevent processing the same transaction more than once
            //Set<Hash> visitedParents = new HashSet<>(Collections.singleton(Hash.NULL_HASH));

            // gather some statistics
            int deletedParentTransactions = 0;

            // iterate through our queue and process all elements (while we iterate we add more)
            Hash parentTransactionHash;
            while((parentTransactionHash = parentTransactionsToCheck.poll()) != null) {
                if(parentTransactionHash.toString().equals("WTQYOYGUUDPCF9WYGLVJBL9IJIYRFNXCFEMYLSDRWAOYLQHQSKBWH9ENNXHNJGGX9TRBXCNAJCWZA9999")) {
                    sideTangleTransactionsSeen++;
                }

                // check if we see this transaction the first time
                if(visitedParents.add(parentTransactionHash)) {
                    if(parentTransactionHash.toString().equals("WTQYOYGUUDPCF9WYGLVJBL9IJIYRFNXCFEMYLSDRWAOYLQHQSKBWH9ENNXHNJGGX9TRBXCNAJCWZA9999")) {
                        sideTangleTransactionsDeleted++;
                    }

                    // retrieve the child transaction
                    TransactionViewModel parentTransaction = TransactionViewModel.fromHash(
                        instance.tangle,
                        parentTransactionHash
                    );

                    // check if the transaction is not confirmed yet and if it references an "older" milestone
                    if(
                        parentTransaction.snapshotIndex() == 0 /*&&
                        parentTransaction.referencedSnapshot(instance.tangle) < currentMilestone.index()*/
                    ) {
                        // if we have "parents" -> queue them to check them as well
                        for(Hash approverHash: parentTransaction.getApprovers(instance.tangle).getHashes()) {
                            parentTransactionsToCheck.add(approverHash);
                        }

                        // increase our cleaned transactions counter (for debugging)
                        totalDeletedTransactions++;
                        deletedParentTransactions++;

                        // TODO: ACTUALLY DELETE THE TRANSACTION

                        // output statistics (for debugging)
                        dumpProgressStatistics();
                    }
                }
            }

            // gather some statistics
            maxDeletedParentTransactions = Math.max(maxDeletedParentTransactions, deletedParentTransactions);
            //endregion

            // go to the next milestone chunk
            currentMilestone = MilestoneViewModel.get(instance.tangle, currentMilestone.index() - 1);
        }

        // output statistics (for debugging)
        dumpFinalStatistics();

        return true;
    }

    public void dumpProgressStatistics() {
        if(totalDeletedTransactions % 10000 == 0) {
            double progress = ((latestMilestoneIndex - currentMilestoneIndex) * 1.0 / (latestMilestoneIndex - instance.milestone.milestoneStartIndex)) * 100.0;

            System.out.println("= PROGRESS (" + String.format("%02.2f", progress) + " %) ===================================");
            System.out.println("| TOTAL DELETED: " + totalDeletedTransactions);
            System.out.println("| MAX DELETED CHILDREN (PER STEP / MILESTONE): " + maxDeletedChildTransactions);
            System.out.println("| MAX DELETED PARENTS (PER STEP / MILESTONE): " + maxDeletedParentTransactions);
            System.out.println("| SIDE TANGLE TRANSACTIONS (SEEN): " + sideTangleTransactionsSeen);
            System.out.println("| SIDE TANGLE TRANSACTIONS (DELETED): " + sideTangleTransactionsDeleted);
            System.out.println("========================================================");
        }
    }

    public void dumpFinalStatistics() {
        System.out.println("= DONE =================================================");
        System.out.println("| TOTAL DELETED: " + totalDeletedTransactions);
        System.out.println("| MAX DELETED CHILDREN (PER STEP / MILESTONE): " + maxDeletedChildTransactions);
        System.out.println("| MAX DELETED PARENTS (PER STEP / MILESTONE): " + maxDeletedParentTransactions);
        System.out.println("| SIDE TANGLE TRANSACTIONS (SEEN): " + sideTangleTransactionsSeen);
        System.out.println("| SIDE TANGLE TRANSACTIONS (DELETED): " + sideTangleTransactionsDeleted);
        System.out.println("========================================================");
    }
}
