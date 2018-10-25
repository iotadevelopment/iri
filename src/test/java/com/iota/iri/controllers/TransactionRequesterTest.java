package com.iota.iri.controllers;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.model.Hash;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.impl.SnapshotProviderImpl;
import com.iota.iri.storage.Tangle;
import com.iota.iri.zmq.MessageQ;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by paul on 5/2/17.
 */
public class TransactionRequesterTest {
    private static Tangle tangle = new Tangle();
    private static SnapshotProvider snapshotProvider;
    private MessageQ mq = null;

    @Before
    public void setUp() throws Exception {
        snapshotProvider = new SnapshotProviderImpl(new MainnetConfig());
    }

    @After
    public void tearDown() {
        snapshotProvider.shutdown();
    }

    @Test
    public void init() {

    }

    @Test
    public void rescanTransactionsToRequest() {

    }

    @Test
    public void getRequestedTransactions() {

    }

    @Test
    public void numberOfTransactionsToRequest() {

    }

    @Test
    public void clearTransactionRequest() {

    }

    @Test
    public void requestTransaction() {

    }

    @Test
    public void transactionToRequest() {

    }

    @Test
    public void checkSolidity() {

    }

    @Test
    public void instance() {

    }

    @Test
    public void nonMilestoneCapacityLimited() throws Exception {
        TransactionRequester txReq = new TransactionRequester(tangle, snapshotProvider.getInitialSnapshot(), mq);
        int capacity = TransactionRequester.MAX_TX_REQ_QUEUE_SIZE;
        //fill tips list
        for (int i = 0; i < capacity * 2 ; i++) {
            Hash hash = TransactionViewModelTest.getRandomTransactionHash();
            txReq.requestTransaction(hash,false);
        }
        //check that limit wasn't breached
        assertEquals(capacity, txReq.numberOfTransactionsToRequest());
    }

    @Test
    public void milestoneCapacityNotLimited() throws Exception {
        TransactionRequester txReq = new TransactionRequester(tangle, snapshotProvider.getInitialSnapshot(), mq);
        int capacity = TransactionRequester.MAX_TX_REQ_QUEUE_SIZE;
        //fill tips list
        for (int i = 0; i < capacity * 2 ; i++) {
            Hash hash = TransactionViewModelTest.getRandomTransactionHash();
            txReq.requestTransaction(hash,true);
        }
        //check that limit was surpassed
        assertEquals(capacity * 2, txReq.numberOfTransactionsToRequest());
    }

    @Test
    public void mixedCapacityLimited() throws Exception {
        TransactionRequester txReq = new TransactionRequester(tangle, snapshotProvider.getInitialSnapshot(), mq);
        int capacity = TransactionRequester.MAX_TX_REQ_QUEUE_SIZE;
        //fill tips list
        for (int i = 0; i < capacity * 4 ; i++) {
            Hash hash = TransactionViewModelTest.getRandomTransactionHash();
            txReq.requestTransaction(hash, (i % 2 == 1));

        }
        //check that limit wasn't breached
        assertEquals(capacity + capacity * 2, txReq.numberOfTransactionsToRequest());
    }

}
