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

    public void traverseApprovers(TransactionViewModel startingTransaction, TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer) throws Exception {
        traverseApprovers(startingTransaction, condition, currentTransactionConsumer, new HashSet<>());
    }

    public void traverseApprovers(TransactionViewModel startingTransaction, TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer, Set<Hash> processedTransactions) throws Exception {
        final Queue<TransactionViewModel> transactionsToExamine = new LinkedList<>();
        startingTransaction.getApprovers(tangle).getHashes().stream().forEach(approverHash -> {
            try {
                transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, approverHash));
            } catch(Exception e) { /* do nothing - just ignore the tx */ }
        });

        TransactionViewModel currentTransaction;
        while((currentTransaction = transactionsToExamine.poll()) != null) {
            if(
                processedTransactions.add(currentTransaction.getHash()) &&
                currentTransaction.getType() != TransactionViewModel.PREFILLED_SLOT &&
                condition.check(currentTransaction)
            ) {
                currentTransactionConsumer.consume(currentTransaction);

                currentTransaction.getApprovers(tangle).getHashes().stream().forEach(approverHash -> {
                    try {
                        transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, approverHash));
                    } catch(Exception e) { /* do nothing - just ignore the tx */ }
                });
            }
        }
    }

    /**
     * This method offers a generic way of traversing the DAG in a depth first way towards the approvees.
     *
     *
     *
     * @param startingTransaction the starting point of the traversal
     * @param condition lambda expression that is used to check when to abort the traversal
     * @param currentTransactionConsumer lambda expression that is used to process the visited transactions
     * @param processedTransactions
     * @throws Exception
     */
    public void traverseApprovees(TransactionViewModel startingTransaction, TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer, Set<Hash> processedTransactions) throws Exception {
        final Queue<TransactionViewModel> transactionsToExamine = new LinkedList<>();
        transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, startingTransaction.getBranchTransactionHash()));
        transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, startingTransaction.getBranchTransactionHash()));

        TransactionViewModel currentTransaction;
        while((currentTransaction = transactionsToExamine.poll()) != null) {
            if(
                processedTransactions.add(currentTransaction.getHash()) &&
                currentTransaction.getType() != TransactionViewModel.PREFILLED_SLOT &&
                condition.check(currentTransaction)
            ) {
                currentTransactionConsumer.consume(currentTransaction);

                transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, currentTransaction.getBranchTransactionHash()));
                transactionsToExamine.add(TransactionViewModel.fromHash(tangle, snapshotManager, currentTransaction.getTrunkTransactionHash()));
            }
        }
    }

    public void traverseApprovees(TransactionViewModel startingTransaction, TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer) throws Exception {
        traverseApprovees(startingTransaction, condition, currentTransactionConsumer, new HashSet<>());
    }

    public void traverseApprovees(Hash startingTransactionHash, TraversalCondition condition, TraversalConsumer currentTransactionConsumer) throws Exception {
        traverseApprovees(TransactionViewModel.fromHash(tangle, snapshotManager, startingTransactionHash), condition, currentTransactionConsumer);
    }

    public void traverseApprovees(Hash startingTransactionHash, TraversalCondition condition, TraversalConsumer currentTransactionConsumer, Set<Hash> processedTransactions) throws Exception {
        traverseApprovees(TransactionViewModel.fromHash(tangle, snapshotManager, startingTransactionHash), condition, currentTransactionConsumer, processedTransactions);
    }

    public void traverseApprovees(MilestoneViewModel milestoneViewModel, TraversalCondition condition, TraversalConsumer currentTransactionConsumer) throws Exception {
        traverseApprovees(milestoneViewModel.getHash(), condition, currentTransactionConsumer, new HashSet<Hash>());
    }

    public void traverseApprovees(MilestoneViewModel milestoneViewModel, TraversalCondition condition, TraversalConsumer currentTransactionConsumer, Set<Hash> processedTransactions) throws Exception {
        traverseApprovees(milestoneViewModel.getHash(), condition, currentTransactionConsumer, processedTransactions);
    }
}
