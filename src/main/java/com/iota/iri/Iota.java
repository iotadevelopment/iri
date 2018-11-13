package com.iota.iri;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.conf.TipSelConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.network.Node;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.network.UDPReceiver;
import com.iota.iri.network.replicator.Replicator;
import com.iota.iri.service.TipsSolidifier;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.milestone.LatestSolidMilestoneTracker;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.milestone.MilestoneSolidifier;
import com.iota.iri.service.milestone.SeenMilestonesRetriever;
import com.iota.iri.service.milestone.impl.LatestMilestoneTrackerImpl;
import com.iota.iri.service.milestone.impl.LatestSolidMilestoneTrackerImpl;
import com.iota.iri.service.milestone.impl.MilestoneServiceImpl;
import com.iota.iri.service.milestone.impl.MilestoneSolidifierImpl;
import com.iota.iri.service.milestone.impl.SeenMilestonesRetrieverImpl;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.LocalSnapshotManager;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.service.snapshot.impl.LocalSnapshotManagerImpl;
import com.iota.iri.service.snapshot.impl.SnapshotProviderImpl;
import com.iota.iri.service.snapshot.impl.SnapshotServiceImpl;
import com.iota.iri.service.tipselection.EntryPointSelector;
import com.iota.iri.service.tipselection.RatingCalculator;
import com.iota.iri.service.tipselection.TailFinder;
import com.iota.iri.service.tipselection.TipSelector;
import com.iota.iri.service.tipselection.Walker;
import com.iota.iri.service.tipselection.impl.CumulativeWeightCalculator;
import com.iota.iri.service.tipselection.impl.EntryPointSelectorImpl;
import com.iota.iri.service.tipselection.impl.TailFinderImpl;
import com.iota.iri.service.tipselection.impl.TipSelectorImpl;
import com.iota.iri.service.tipselection.impl.WalkerAlpha;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPruningException;
import com.iota.iri.service.transactionpruning.async.AsyncTransactionPruner;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.ZmqPublishProvider;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import com.iota.iri.utils.Pair;
import com.iota.iri.zmq.MessageQ;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;

/**
 * Created by paul on 5/19/17.
 */
public class Iota {
    private static final Logger log = LoggerFactory.getLogger(Iota.class);

    public final LedgerValidator ledgerValidator;
    public final LatestMilestoneTracker latestMilestoneTracker;
    public final LatestSolidMilestoneTracker latestSolidMilestoneTracker;
    public final MilestoneService milestoneService;
    public final MilestoneSolidifier milestoneSolidifier;
    public final Tangle tangle;
    public final SeenMilestonesRetriever seenMilestonesRetriever;
    public final SnapshotService snapshotService;
    public final SnapshotProvider snapshotProvider;
    public final TransactionPruner transactionPruner;
    public final LocalSnapshotManager localSnapshotManager;
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

    public Iota(IotaConfig configuration) throws SnapshotException, TransactionPruningException {
        this.configuration = configuration;

        // initialize services (least amount of dependencies)
        snapshotService = new SnapshotServiceImpl();
        milestoneService = new MilestoneServiceImpl(snapshotService);

        // initialize remaining
        tangle = new Tangle();
        tipsViewModel = new TipsViewModel();

        messageQ = MessageQ.createWith(configuration);
        snapshotProvider = new SnapshotProviderImpl(configuration);
        transactionPruner = new AsyncTransactionPruner(tangle, tipsViewModel, snapshotProvider.getInitialSnapshot(), configuration);
        transactionPruner.restoreState();
        localSnapshotManager = new LocalSnapshotManagerImpl(snapshotProvider, snapshotService, transactionPruner, tangle, configuration);
        transactionRequester = new TransactionRequester(tangle, snapshotProvider.getInitialSnapshot(), messageQ);
        seenMilestonesRetriever = new SeenMilestonesRetrieverImpl(tangle, snapshotProvider, transactionRequester);
        transactionValidator = new TransactionValidator(tangle, snapshotProvider.getInitialSnapshot(), tipsViewModel, transactionRequester);
        milestoneSolidifier = new MilestoneSolidifierImpl(snapshotProvider.getInitialSnapshot(), transactionValidator);
        ledgerValidator = new LedgerValidator(tangle, snapshotProvider, snapshotService, milestoneService, transactionRequester, messageQ);
        tipsSolidifier = new TipsSolidifier(tangle, transactionValidator, tipsViewModel);
        tipsSelector = createTipSelector(configuration);
        latestMilestoneTracker = new LatestMilestoneTrackerImpl(tangle, snapshotProvider, snapshotService, milestoneService, milestoneSolidifier, messageQ, configuration);
        latestSolidMilestoneTracker = new LatestSolidMilestoneTrackerImpl(tangle, snapshotProvider, milestoneService, latestMilestoneTracker, ledgerValidator, messageQ);
        node = new Node(tangle, snapshotProvider.getInitialSnapshot(), transactionValidator, transactionRequester, tipsViewModel, latestMilestoneTracker, messageQ,
                configuration);
        replicator = new Replicator(node, configuration);
        udpReceiver = new UDPReceiver(node, configuration);
    }

    public void init() throws Exception {
        initializeTangle();
        tangle.init();

        if (configuration.isRescanDb()){
            rescanDb();
        }

        if (configuration.isRevalidate()) {
            tangle.clearColumn(com.iota.iri.model.persistables.Milestone.class);
            tangle.clearColumn(com.iota.iri.model.StateDiff.class);
            tangle.clearMetadata(com.iota.iri.model.persistables.Transaction.class);
        }
        latestMilestoneTracker.start();
        latestSolidMilestoneTracker.start();
        seenMilestonesRetriever.start();
        milestoneSolidifier.start();
        transactionValidator.init(configuration.isTestnet(), configuration.getMwm());
        tipsSolidifier.init();
        transactionRequester.init(configuration.getpRemoveRequest());
        udpReceiver.init();
        replicator.init();
        node.init();
        if (configuration.getLocalSnapshotsEnabled()) {
            localSnapshotManager.start(latestMilestoneTracker);

            if (configuration.getLocalSnapshotsPruningEnabled()) {
                 ((AsyncTransactionPruner) transactionPruner).start();
            }
        }
    }

    private void rescanDb() throws Exception {
        //delete all transaction indexes
        tangle.clearColumn(com.iota.iri.model.persistables.Address.class);
        tangle.clearColumn(com.iota.iri.model.persistables.Bundle.class);
        tangle.clearColumn(com.iota.iri.model.persistables.Approvee.class);
        tangle.clearColumn(com.iota.iri.model.persistables.ObsoleteTag.class);
        tangle.clearColumn(com.iota.iri.model.persistables.Tag.class);
        tangle.clearColumn(com.iota.iri.model.persistables.Milestone.class);
        tangle.clearColumn(com.iota.iri.model.StateDiff.class);
        tangle.clearMetadata(com.iota.iri.model.persistables.Transaction.class);

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
        latestMilestoneTracker.shutdown();
        latestSolidMilestoneTracker.shutdown();
        seenMilestonesRetriever.shutdown();
        milestoneSolidifier.shutdown();
        tipsSolidifier.shutdown();
        node.shutdown();
        udpReceiver.shutdown();
        replicator.shutdown();
        transactionValidator.shutdown();
        tangle.shutdown();
        messageQ.shutdown();
        snapshotProvider.shutdown();

        if (configuration.getLocalSnapshotsEnabled()) {
            localSnapshotManager.shutdown();

            if (configuration.getLocalSnapshotsPruningEnabled()) {
                ((AsyncTransactionPruner) transactionPruner).shutdown();
            }
        }
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
        if (configuration.isZmqEnabled()) {
            tangle.addPersistenceProvider(new ZmqPublishProvider(messageQ));
        }
    }

    private TipSelector createTipSelector(TipSelConfig config) {
        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, snapshotProvider);
        RatingCalculator ratingCalculator = new CumulativeWeightCalculator(tangle, snapshotProvider.getInitialSnapshot());
        TailFinder tailFinder = new TailFinderImpl(tangle);
        Walker walker = new WalkerAlpha(tailFinder, tangle, messageQ, new SecureRandom(), config);
        return new TipSelectorImpl(tangle, snapshotProvider, ledgerValidator, entryPointSelector, ratingCalculator,
                walker, config);
    }
}
