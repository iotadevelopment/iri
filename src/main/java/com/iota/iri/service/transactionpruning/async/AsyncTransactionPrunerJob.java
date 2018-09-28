package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.storage.Tangle;

public abstract class AsyncTransactionPrunerJob implements TransactionPrunerJob {
    private TransactionPruner transactionPruner;

    /**
     * Holds a reference to the tangle object which acts as a database interface.
     */
    private Tangle tangle;

    private TipsViewModel tipsViewModel;

    private Snapshot snapshot;

    @Override
    public void setTransactionPruner(TransactionPruner transactionPruner) {
        this.transactionPruner = transactionPruner;
    }

    @Override
    public TransactionPruner getTransactionPruner() {
        return transactionPruner;
    }

    @Override
    public void setTangle(Tangle tangle) {
        this.tangle = tangle;
    }

    @Override
    public Tangle getTangle() {
        return tangle;
    }

    @Override
    public void setTipsViewModel(TipsViewModel tipsViewModel) {
        this.tipsViewModel = tipsViewModel;
    }

    @Override
    public TipsViewModel getTipsViewModel() {
        return tipsViewModel;
    }

    @Override
    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public Snapshot getSnapshot() {
        return snapshot;
    }
}
