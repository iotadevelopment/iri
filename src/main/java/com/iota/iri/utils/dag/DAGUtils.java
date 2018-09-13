package com.iota.iri.utils.dag;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;

import java.util.*;

/**
 * This class offers generic functions for recurring tasks that are related to the tangle and that otherwise would have
 * to be implemented over and over again in different parts of the code.
 */
public class DAGUtils {
    /**
     * Holds references to the singleton DAGUtils instances.
     */
    protected static HashMap<Tangle, DAGUtils> instances = new HashMap<>();

    /**
     * Holds a reference to the tangle instance which acts as an interface to the used database.
     */
    protected Tangle tangle;

    /**
     * This method allows us to retrieve the DAGUtils instance that corresponds to the given parameters.
     *
     * To save memory and to be able to use things like caches effectively, we do not allow to create multiple instances
     * of this class for the same set of parameters, but create a singleton instance of it.
     *
     * @param tangle database interface used to retrieve data
     * @return DAGUtils instance that allows to use the implemented methods.
     */
    public static DAGUtils get(Tangle tangle) {
        DAGUtils instance = instances.get(tangle);
        if(instance == null) {
            instance = new DAGUtils(tangle);

            instances.put(tangle, instance);
        }

        return instance;
    }

    /**
     * Constructor of the class which allows to provide the required dependencies for the implemented methods.
     *
     * Since the constructor is protected we are not able to manually create instances of this class and have to
     * retrieve them through their singleton accessor {@link #get}.
     *
     * @param tangle database interface used to retrieve data
     */
    protected DAGUtils(Tangle tangle) {
        this.tangle = tangle;
    }

    //region TRAVERSE APPROVERS (BOTTOM -> TOP) ////////////////////////////////////////////////////////////////////////

    /**
     * This method provides a generic interface for traversing all approvers of a transaction up to a given point.
     *
     * To make the use of this method as easy as possible we provide a broad range of possible parameter signatures, so
     * we do not have to manually convert between milestones, transactions and hashes (see other methods with the same
     * name).
     *
     * It uses an non-recursive iterative algorithm that is able to handle huge chunks of the tangle without running out
     * of memory. It creates a queue of transactions that are being examined and processes them one by one. As new
     * approvers are found, they will be added to the queue and processed accordingly.
     *
     * Every found transaction is passed into the provided condition lambda, to determine if it still belongs to the
     * desired set of transactions and only then will be passed on to the currentTransactionConsumer lambda.
     *
     * @param startingTransactionHash the starting point of the traversal
     * @param condition predicate that allows to control how long the traversal should continue (receives the current
     *                  transaction as a parameter)
     * @param currentTransactionConsumer a lambda function that allows us to "process" the found transactions
     * @param processedTransactions a set of hashes that shall be considered as "processed" already and that will
     *                              consequently be ignored in the traversal
     * @throws TraversalException if anything goes wrong while traversing the graph and processing the transactions
     */
    public void traverseApprovers(Hash startingTransactionHash,
                                  TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer,
                                  Set<Hash> processedTransactions) throws TraversalException {
        final Queue<Hash> transactionsToExamine = new LinkedList<>(Collections.singleton(startingTransactionHash));
        try {
            Hash currentTransactionHash;
            while((currentTransactionHash = transactionsToExamine.poll()) != null) {
                if(processedTransactions.add(currentTransactionHash)) {
                    TransactionViewModel currentTransaction = TransactionViewModel.fromHash(tangle, currentTransactionHash);
                    if(
                    currentTransaction.getType() != TransactionViewModel.PREFILLED_SLOT && (
                    // do not "check" the starting transaction since it is not an "approver"
                    currentTransactionHash == startingTransactionHash ||
                    condition.check(currentTransaction)
                    )
                    ) {
                        // do not consume the starting transaction since it is not an "approver"
                        if(currentTransactionHash != startingTransactionHash) {
                            currentTransactionConsumer.consume(currentTransaction);
                        }

                        currentTransaction.getApprovers(tangle).getHashes().stream().forEach(approverHash -> transactionsToExamine.add(approverHash));
                    }
                }
            }
        } catch (Exception e) {
            throw new TraversalException("error while traversing the approvers of transaction " + startingTransactionHash, e);
        }
    }

    /**
     * Works like {@link DAGUtils#traverseApprovers(Hash, TraversalCondition, TraversalConsumer, Set)}
     * but defaults to an empty set of processed transactions to consider all transactions.
     *
     * @see DAGUtils#traverseApprovers(Hash, TraversalCondition, TraversalConsumer, Set)
     *
     * @param startingTransactionHash the starting point of the traversal
     * @param condition predicate that allows to control how long the traversal should continue (receives the current
     *                  transaction as a parameter)
     * @param currentTransactionConsumer a lambda function that allows us to "process" the found transactions
     * @throws TraversalException if anything goes wrong while traversing the graph and processing the transactions
     */
    public void traverseApprovers(Hash startingTransactionHash,
                                  TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer) throws TraversalException {
        traverseApprovers(startingTransactionHash, condition, currentTransactionConsumer, new HashSet<>());
    }

    //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////////

    //region TRAVERSE APPROVEES (TOP -> BOTTOM) ////////////////////////////////////////////////////////////////////////

    /**
     * This method provides a generic interface for traversing all approvees of a transaction down to a given point.
     *
     * To make the use of this method as easy as possible we provide a broad range of possible parameter signatures, so
     * we do not have to manually convert between milestones, transactions and hashes (see other methods with the same
     * name).
     *
     * It uses an non-recursive iterative algorithm that is able to handle huge chunks of the tangle without running out
     * of memory. It creates a queue of transactions that are being examined and processes them one by one. As new
     * approvees are found, they will be added to the queue and processed accordingly.
     *
     * Every found transaction is passed into the provided condition lambda, to determine if it still belongs to the
     * desired set of transactions and only then will be passed on to the currentTransactionConsumer lambda.
     *
     * @param startingTransactionHash the starting point of the traversal
     * @param condition predicate that allows to control how long the traversal should continue (receives the current
     *                  transaction as a parameter)
     * @param currentTransactionConsumer a lambda function that allows us to "process" the found transactions
     * @param processedTransactions a set of hashes that shall be considered as "processed" already and that will
     *                              consequently be ignored in the traversal
     * @throws TraversalException if anything goes wrong while traversing the graph and processing the transactions
     */
    public void traverseApprovees(Hash startingTransactionHash,
                                  TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer,
                                  Set<Hash> processedTransactions) throws TraversalException {
        final Queue<Hash> transactionsToExamine = new LinkedList<>(Collections.singleton(startingTransactionHash));
        try {
            Hash currentTransactionHash;
            while((currentTransactionHash = transactionsToExamine.poll()) != null) {
                if(processedTransactions.add(currentTransactionHash)) {
                    TransactionViewModel currentTransaction = TransactionViewModel.fromHash(tangle, currentTransactionHash);
                    if(
                    currentTransaction.getType() != TransactionViewModel.PREFILLED_SLOT &&(
                    // do not "check" the starting transaction since it is not an "approvee"
                    currentTransactionHash == startingTransactionHash ||
                    condition.check(currentTransaction)
                    )
                    ) {
                        // do not consume the starting transaction since it is not an "approvee"
                        if(currentTransactionHash != startingTransactionHash) {
                            currentTransactionConsumer.consume(currentTransaction);
                        }

                        transactionsToExamine.add(currentTransaction.getBranchTransactionHash());
                        transactionsToExamine.add(currentTransaction.getTrunkTransactionHash());
                    }
                }
            }
        } catch (Exception e) {
            throw new TraversalException("error while traversing the approvees of transaction " + startingTransactionHash, e);
        }
    }

    /**
     * Works like {@link DAGUtils#traverseApprovees(Hash, TraversalCondition, TraversalConsumer, Set)}
     * but defaults to an empty set of processed transactions to consider all transactions.
     *
     * @see DAGUtils#traverseApprovees(Hash, TraversalCondition, TraversalConsumer, Set)
     *
     * @param startingTransactionHash the starting point of the traversal
     * @param condition predicate that allows to control how long the traversal should continue (receives the current
     *                  transaction as a parameter)
     * @param currentTransactionConsumer a lambda function that allows us to "process" the found transactions
     * @throws TraversalException if anything goes wrong while traversing the graph and processing the transactions
     */
    public void traverseApprovees(Hash startingTransactionHash,
                                  TraversalCondition condition,
                                  TraversalConsumer currentTransactionConsumer) throws TraversalException {
        traverseApprovees(startingTransactionHash, condition, currentTransactionConsumer, new HashSet<>());
    }

    //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////////
}
