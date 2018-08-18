package com.iota.iri;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.conf.TipSelConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.network.Node;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.network.UDPReceiver;
import com.iota.iri.network.replicator.Replicator;
import com.iota.iri.service.TipsSolidifier;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.service.tipselection.*;
import com.iota.iri.service.tipselection.impl.*;
import com.iota.iri.storage.*;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import com.iota.iri.utils.Pair;
import com.iota.iri.zmq.MessageQ;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.SecureRandom;
import java.util.List;

/**
 * Created by paul on 5/19/17.
 */
public class Iota {
    private static final Logger log = LoggerFactory.getLogger(Iota.class);

    public Tangle tangle = null;
    public SnapshotManager snapshotManager = null;

    public final LedgerValidator ledgerValidator;
    public final MilestoneTracker milestone;
    public final TransactionValidator transactionValidator;
    public final TipsSolidifier tipsSolidifier;
    public final TransactionRequester transactionRequester;
    public final Node node;
    public final UDPReceiver udpReceiver;
    public final Replicator replicator;
    public final IotaConfig configuration;
    public final TipsViewModel tipsViewModel;
    public final MessageQ messageQ;
    public final TipSelector tipsSelector;

    public Iota(IotaConfig configuration) throws IOException {
        this.configuration = configuration;
        tangle = new Tangle();
        snapshotManager = new SnapshotManager(tangle, configuration);
        messageQ = MessageQ.createWith(configuration);
        tipsViewModel = new TipsViewModel();
        transactionRequester = new TransactionRequester(tangle, messageQ);
        transactionValidator = new TransactionValidator(tangle, snapshotManager, tipsViewModel, transactionRequester, messageQ,
                configuration);
        milestone = new MilestoneTracker(tangle, snapshotManager, transactionValidator, messageQ, configuration);
        node = new Node(tangle, snapshotManager, transactionValidator, transactionRequester, tipsViewModel, milestone, messageQ,
                configuration);
        replicator = new Replicator(node, configuration);
        udpReceiver = new UDPReceiver(node, configuration);
        ledgerValidator = new LedgerValidator(tangle, snapshotManager, milestone, transactionRequester, messageQ);
        tipsSolidifier = new TipsSolidifier(tangle, snapshotManager, transactionValidator, tipsViewModel);
        tipsSelector = createTipSelector(configuration);
    }

    public void init() throws Exception {
        initializeTangle();
        tangle.init();

        if (configuration.isRescanDb()){
            rescan_db();
        }

        if (configuration.isRevalidate()) {
            tangle.clearColumn(com.iota.iri.model.Milestone.class);
            tangle.clearColumn(com.iota.iri.model.StateDiff.class);
            tangle.clearMetadata(com.iota.iri.model.Transaction.class);
        }
        milestone.init(SpongeFactory.Mode.CURLP27, ledgerValidator);
        transactionValidator.init(configuration.isTestnet(), configuration.getMwm());
        tipsSolidifier.init();
        transactionRequester.init(configuration.getpRemoveRequest());
        udpReceiver.init();
        replicator.init();
        node.init();
        snapshotManager.init(milestone);
    }

    private void rescan_db() throws Exception {
        //delete all transaction indexes
        tangle.clearColumn(com.iota.iri.model.Address.class);
        tangle.clearColumn(com.iota.iri.model.Bundle.class);
        tangle.clearColumn(com.iota.iri.model.Approvee.class);
        tangle.clearColumn(com.iota.iri.model.ObsoleteTag.class);
        tangle.clearColumn(com.iota.iri.model.Tag.class);
        tangle.clearColumn(com.iota.iri.model.Milestone.class);
        tangle.clearColumn(com.iota.iri.model.StateDiff.class);
        tangle.clearMetadata(com.iota.iri.model.Transaction.class);

        //rescan all tx & refill the columns
        TransactionViewModel tx = TransactionViewModel.first(tangle);
        int counter = 0;
        while (tx != null) {
            if (++counter % 10000 == 0) {
                log.info("Rescanned {} Transactions", counter);
            }
            List<Pair<Indexable, Persistable>> saveBatch = tx.getSaveBatch();
            saveBatch.remove(5);
            tangle.saveBatch(saveBatch);
            tx = tx.next(tangle);
        }
    }

    public void shutdown() throws Exception {
        milestone.shutDown();
        tipsSolidifier.shutdown();
        node.shutdown();
        udpReceiver.shutdown();
        replicator.shutdown();
        transactionValidator.shutdown();
        tangle.shutdown();
        messageQ.shutdown();
        snapshotManager.shutDown();
    }

    private void initializeTangle() {
        switch (configuration.getMainDb()) {
            case "rocksdb": {
                tangle.addPersistenceProvider(new RocksDBPersistenceProvider(
                        configuration.getDbPath(),
                        configuration.getDbLogPath(),
                        configuration.getDbCacheSize()));
                break;
            }
            default: {
                throw new NotImplementedException("No such database type.");
            }
        }
        if (configuration.isExport()) {
            tangle.addPersistenceProvider(new FileExportProvider());
        }
        if (configuration.isZmqEnabled()) {
            tangle.addPersistenceProvider(new ZmqPublishProvider(messageQ));
        }
    }

    private TipSelector createTipSelector(TipSelConfig config) {
        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, snapshotManager, milestone, config);
        RatingCalculator ratingCalculator = new CumulativeWeightCalculator(tangle, snapshotManager);
        TailFinder tailFinder = new TailFinderImpl(tangle, snapshotManager);
        Walker walker = new WalkerAlpha(tailFinder, tangle, messageQ, new SecureRandom(), config);
        return new TipSelectorImpl(tangle, snapshotManager, ledgerValidator, entryPointSelector, ratingCalculator,
                walker, milestone, config);
    }
}
