package com.iota.iri.utils.dag;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.storage.Tangle;

import java.util.*;

public class DAGUtils {
    protected Tangle tangle;

    protected SnapshotManager snapshotManager;

    public static DAGUtils get(Tangle tangle, SnapshotManager snapshotManager) {
        return new DAGUtils(tangle, snapshotManager);
    }

    public DAGUtils(Tangle tangle, SnapshotManager snapshotManager) {
        this.tangle = tangle;
        this.snapshotManager = snapshotManager;
    }

    public void traverseApprovers(TransactionViewModel startingTransaction, TraversalCondition condition, TraversalConsumer currentTransactionConsumer) throws Exception {

    }

    /**
     *
     * @param condition tust
     * @param currentTransactionConsumer tost
     */
    public void traverseApprovees(TransactionViewModel startingTransaction, TraversalCondition condition, TraversalConsumer currentTransactionConsumer) throws Exception {
        // create a set where we collect the solid entry points
        Set<Hash> seenTransactions = new HashSet<>();

        // create a queue where we collect the transactions that shall be examined (starting with our milestone)
        final Queue<TransactionViewModel> transactionsToExamine = new LinkedList<>(Collections.singleton(startingTransaction));

        // traverse the transactions
        TransactionViewModel currentTransaction;
        while((currentTransaction = transactionsToExamine.poll()) != null) {
            if(
                seenTransactions.add(currentTransaction.getHash()) &&
                currentTransaction.getType() != TransactionViewModel.PREFILLED_SLOT &&
                condition.check(currentTransaction)
            ) {
                currentTransactionConsumer.consume(currentTransaction);

                transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, currentTransaction.getBranchTransactionHash()));
                transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, currentTransaction.getTrunkTransactionHash()));
            }
        }
    }

    public void traverseApprovees(Hash startingTransactionHash, TraversalCondition condition, TraversalConsumer currentTransactionConsumer) throws Exception {
        traverseApprovees(TransactionViewModel.fromHash(tangle, snapshotManager, startingTransactionHash), condition, currentTransactionConsumer);
    }

    public void traverseApprovees(MilestoneViewModel milestoneViewModel, TraversalCondition condition, TraversalConsumer currentTransactionConsumer) throws Exception {
        traverseApprovees(milestoneViewModel.getHash(), condition, currentTransactionConsumer);
    }
}
