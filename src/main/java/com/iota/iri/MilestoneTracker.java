package com.iota.iri;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.AddressViewModel;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hash.Curl;
import com.iota.iri.hash.ISS;
import com.iota.iri.hash.ISSInPlace;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.StateDiff;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.milestone.MilestoneSolidifier;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.impl.SnapshotManager;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Converter;
import com.iota.iri.utils.ProgressLogger;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.zmq.MessageQ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.iota.iri.MilestoneTracker.Status.INITIALIZED;
import static com.iota.iri.MilestoneTracker.Status.INITIALIZING;
import static com.iota.iri.MilestoneTracker.Validity.INCOMPLETE;
import static com.iota.iri.MilestoneTracker.Validity.INVALID;
import static com.iota.iri.MilestoneTracker.Validity.VALID;

public class MilestoneTracker {
    /**
     * Validity states of transactions regarding their milestone status.
     */
    public enum Status {
        INITIALIZING,
        INITIALIZED
    }

    /**
     * Validity states of transactions regarding their milestone status.
     */
    enum Validity {
        VALID,
        INVALID,
        INCOMPLETE
    }

    protected Status status = INITIALIZING;

    private static int RESCAN_INTERVAL = 5000;

    /**
     * How often (in milliseconds) to dump log messages about status updates.
     */
    private static int STATUS_LOG_INTERVAL = 5000;

    /**
     * This variable is used to keep track of the asynchronous tasks, that the "Solid Milestone Tracker" should wait for.
     */
    private AtomicInteger blockingSolidMilestoneTrackerTasks = new AtomicInteger(0);

    private final Logger log = LoggerFactory.getLogger(MilestoneTracker.class);
    private final Tangle tangle;
    private final SnapshotManager snapshotManager;
    private final Hash coordinator;
    private final TransactionRequester transactionRequester;
    private final boolean testnet;
    private final MessageQ messageQ;
    private final DAGHelper dagHelper;
    private final int numOfKeysInMilestone;
    private final boolean acceptAnyTestnetCoo;
    private final boolean isRescanning;

    private LedgerValidator ledgerValidator;
    public Hash latestMilestone;

    public int latestMilestoneIndex;

    private final Set<Hash> analyzedMilestoneCandidates = new HashSet<>();

    private boolean shuttingDown;

    private MilestoneSolidifier milestoneSolidifier;

    public MilestoneTracker(Tangle tangle,
                            SnapshotManager snapshotManager,
                            TransactionValidator transactionValidator,
                            TransactionRequester transactionRequester,
                            MessageQ messageQ,
                            IotaConfig config
    ) {
        this.tangle = tangle;
        this.snapshotManager = snapshotManager;
        this.transactionRequester = transactionRequester;
        this.messageQ = messageQ;
        this.milestoneSolidifier = new MilestoneSolidifier(snapshotManager, transactionValidator, transactionRequester);
        this.dagHelper = DAGHelper.get(tangle);

        //configure
        this.testnet = config.isTestnet();
        this.coordinator = HashFactory.ADDRESS.create(config.getCoordinator());
        this.numOfKeysInMilestone = config.getNumberOfKeysInMilestone();
        this.acceptAnyTestnetCoo = config.isDontValidateTestnetMilestoneSig();
        this.isRescanning = config.isRescanDb() || config.isRevalidate();
        this.latestMilestoneIndex = snapshotManager.getLatestSnapshot().getIndex();
        this.latestMilestone = snapshotManager.getLatestSnapshot().getHash();
    }

    public Status getStatus() {
        return this.status;
    }

    public void init (LedgerValidator ledgerValidator) {
        this.ledgerValidator = ledgerValidator;

        // to be able to process the milestones in the correct order after a rescan of the database, we initialize
        // this variable with 1 and wait for the "Latest Milestone Tracker" to process all milestones at least once
        if(isRescanning) {
            blockingSolidMilestoneTrackerTasks.incrementAndGet();
        }

        milestoneSolidifier.start();

        // start the threads
        spawnLatestMilestoneTracker();
        spawnSolidMilestoneTracker();
        spawnMilestoneSolidifier();
    }

    private void spawnLatestMilestoneTracker() {
        (new Thread(() -> {
            ProgressLogger scanningMilestonesProgress = new ProgressLogger("Scanning Latest Milestones", log);

            // bootstrap our latestMilestone with the last milestone in the database (faster startup)
            try {
                analyzeMilestoneCandidate(MilestoneViewModel.latest(tangle).getHash());
            } catch(Exception e) { /* do nothing */ }

            LinkedList<Hash> latestMilestoneQueue = new LinkedList<>();
            log.info("Tracker started.");
            boolean firstRun = true;
            while (!shuttingDown) {
                long scanTime = System.currentTimeMillis();

                try {
                    // analyze all found milestone candidates
                    Set<Hash> hashes = AddressViewModel.load(tangle, coordinator).getHashes();

                    for(Hash hash: hashes) {
                        if(!shuttingDown && analyzedMilestoneCandidates.add(hash)) {
                            latestMilestoneQueue.push(hash);
                        }
                    }

                    // process the latest milestones in chunks of 1000 to allow new milestones to be processed as well
                    int i = 0;
                    while(!shuttingDown && i++ < 1000) {
                        Hash currentMilestone;
                        if((currentMilestone = latestMilestoneQueue.poll()) != null) {
                            if(analyzeMilestoneCandidate(currentMilestone) == INCOMPLETE) {
                                analyzedMilestoneCandidates.remove(currentMilestone);
                            }
                        } else {
                            this.status = INITIALIZED;
                        }
                    }

                    if (latestMilestoneQueue.size() >= 1) {
                        log.info("Milestone Tracker: Processing milestones ... " + latestMilestoneQueue.size() + " milestones remaining");
                    }

                    // allow the "Solid Milestone Tracker" to continue if we finished the first run in rescanning mode
                    if(firstRun && isRescanning) {
                        blockingSolidMilestoneTrackerTasks.decrementAndGet();
                    }

                    Thread.sleep(Math.max(1, RESCAN_INTERVAL - (System.currentTimeMillis() - scanTime)));
                } catch (final Exception e) {
                    log.error("Error during Latest Milestone updating", e);
                }

                firstRun = false;
            }
        }, "Latest Milestone Tracker")).start();
    }

    private void spawnSolidMilestoneTracker() {
        (new Thread(() -> {
            log.info("Tracker started.");
            while (!shuttingDown) {
                long scanTime = System.currentTimeMillis();

                try {
                    if(snapshotManager.getLatestSnapshot().getIndex() < latestMilestoneIndex) {
                        updateLatestSolidSubtangleMilestone();
                    }

                    Thread.sleep(Math.max(1, RESCAN_INTERVAL - (System.currentTimeMillis() - scanTime)));
                } catch (final Exception e) {
                    log.error("Error during Solid Milestone updating", e);
                }
            }
        }, "Solid Milestone Tracker")).start();
    }

    private void spawnMilestoneSolidifier() {
        new Thread(() -> {
            // prepare seen milestones for concurrent access
            ConcurrentHashMap<Hash, Integer> seenMilestones = new ConcurrentHashMap<>(snapshotManager.getInitialSnapshot().getSeenMilestones());

            while(!shuttingDown) {
                // retrieve milestones from our local snapshot (if they are still missing)
                seenMilestones.forEach((milestoneHash, milestoneIndex) -> {
                    try {
                        // remove old milestones that are not relevant anymore
                        if(milestoneIndex <= snapshotManager.getLatestSnapshot().getIndex()) {
                            seenMilestones.remove(milestoneHash);
                        }

                        // check milestones that are within our check range
                        else if(milestoneIndex < snapshotManager.getLatestSnapshot().getIndex() + 50) {
                            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(tangle, milestoneHash);
                            if(milestoneTransaction == null || milestoneTransaction.getType() == TransactionViewModel.PREFILLED_SLOT) {
                                transactionRequester.requestTransaction(milestoneHash, true);
                            } else {
                                seenMilestones.remove(milestoneHash);
                            }
                        }
                    } catch(Exception e) { /* do nothing */ }
                });

                try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
            }
        }, "Milestone Solidifier").start();
    }

    /**
     * This method analyzes a transaction to determine if it is a milestone.
     *
     * In contrast to {@link #validateMilestone} it also updates the internal variables keeping track of the latest
     * milestone and dumps a log message whenever a change is detected. So this method can be seen as the core logic for
     * scanning and keeping track of the latest milestones. Since it internally calls {@link #validateMilestone} a
     * milestone will have its corresponding {@link MilestoneViewModel} created after being processed by this method.
     *
     * The method first checks if the transaction originates from the coordinator address, so the check is relatively
     * cheap and can easily performed on any incoming transaction.
     *
     * @param potentialMilestoneTransactionHash hash of the transaction that shall get checked for being a milestone
     * @return VALID if the transaction is a milestone, INCOMPLETE if the bundle is not complete and INVALID otherwise
     * @throws Exception if something goes wrong while retrieving information from the database
     */
    public Validity analyzeMilestoneCandidate(Hash potentialMilestoneTransactionHash) throws Exception {
        return analyzeMilestoneCandidate(TransactionViewModel.fromHash(tangle, potentialMilestoneTransactionHash));
    }

    /**
     * This method analyzes a transaction to determine if it is a milestone.
     *
     * In contrast to {@link #validateMilestone} it also updates the internal variables keeping track of the latest
     * milestone and dumps a log message whenever a change is detected. So this method can be seen as the core logic for
     * scanning and keeping track of the latest milestones. Since it internally calls {@link #validateMilestone} a
     * milestone will have its corresponding {@link MilestoneViewModel} created after being processed by this method.
     *
     * The method first checks if the transaction originates from the coordinator address, so the check is relatively
     * cheap and can easily performed on any incoming transaction.
     *
     * @param potentialMilestoneTransaction transaction that shall get checked for being a milestone
     * @return VALID if the transaction is a milestone, INCOMPLETE if the bundle is not complete and INVALID otherwise
     * @throws Exception if something goes wrong while retrieving information from the database
     */
    public Validity analyzeMilestoneCandidate(TransactionViewModel potentialMilestoneTransaction) throws Exception {
        if (coordinator.equals(potentialMilestoneTransaction.getAddressHash()) && potentialMilestoneTransaction.getCurrentIndex() == 0) {
            int milestoneIndex = getIndex(potentialMilestoneTransaction);

            switch (validateMilestone(SpongeFactory.Mode.CURLP27, 1, potentialMilestoneTransaction, milestoneIndex)) {
                case VALID:
                    if (milestoneIndex > latestMilestoneIndex) {
                        messageQ.publish("lmi %d %d", latestMilestoneIndex, milestoneIndex);
                        log.info("Latest milestone has changed from #" + latestMilestoneIndex + " to #" + milestoneIndex);

                        latestMilestone = potentialMilestoneTransaction.getHash();
                        latestMilestoneIndex = milestoneIndex;
                    } else {
                        MilestoneViewModel latestMilestoneViewModel = MilestoneViewModel.latest(tangle);
                        if (latestMilestoneViewModel.index() > latestMilestoneIndex) {
                            messageQ.publish("lmi %d %d", latestMilestoneIndex, latestMilestoneViewModel.index());
                            log.info("Latest milestone has changed from #" + latestMilestoneIndex + " to #" + latestMilestoneViewModel.index());

                            latestMilestone = latestMilestoneViewModel.getHash();
                            latestMilestoneIndex = latestMilestoneViewModel.index();
                        }
                    }

                    if(!potentialMilestoneTransaction.isSolid()) {
                        milestoneSolidifier.add(potentialMilestoneTransaction.getHash(), milestoneIndex);
                    }

                    potentialMilestoneTransaction.isSnapshot(tangle, snapshotManager, true);

                    return VALID;

                case INCOMPLETE:
                    milestoneSolidifier.add(potentialMilestoneTransaction.getHash(), milestoneIndex);

                    potentialMilestoneTransaction.isSnapshot(tangle, snapshotManager, true);

                    return INCOMPLETE;
            }
        }

        return INVALID;
    }

    public void resetCorruptedMilestone(int milestoneIndex, String identifier) {
        resetCorruptedMilestone(milestoneIndex, identifier, new HashSet<>());
    }

    public void resetCorruptedMilestone(int milestoneIndex, String identifier, HashSet<Hash> processedTransactions) {
        if(milestoneIndex <= snapshotManager.getInitialSnapshot().getIndex()) {
            return;
        }

        System.out.println("REPAIRING: " + snapshotManager.getLatestSnapshot().getIndex() + " <=> " + milestoneIndex + " => " + identifier);

        try {
            MilestoneViewModel milestoneToRepair = MilestoneViewModel.get(tangle, milestoneIndex);

            if(milestoneToRepair != null) {
                // reset the ledger to the state before the erroneous milestone appeared
                if(milestoneToRepair.index() <= snapshotManager.getLatestSnapshot().getIndex()) {
                    snapshotManager.getLatestSnapshot().rollBackMilestones(milestoneToRepair.index(), tangle);
                }

                resetSnapshotIndexOfMilestoneTransactions(milestoneToRepair, processedTransactions);
                tangle.delete(StateDiff.class, milestoneToRepair.getHash());
            }
        } catch (Exception e) {
            log.error("failed to repair corrupted milestone with index #" + milestoneIndex, e);
        }
    }

    /**
     * This method resets the snapshotIndex of all transactions that "belong" to a milestone.
     *
     * While traversing the graph we use the snapshotIndex value to check if it still belongs to the given milestone and
     * ignore all transactions that where referenced by a previous ones. Since we check if the snapshotIndex is bigger or
     * equal to the one of the targetMilestone, we can ignore the case that a previous milestone was not processed, yet
     * since it's milestoneIndex would still be 0.
     *
     * @param currentMilestone the milestone that shall have its confirmed transactions reset
     * @throws Exception if something goes wrong while accessing the database
     */
    public void resetSnapshotIndexOfMilestoneTransactions(MilestoneViewModel currentMilestone, HashSet<Hash> processedTransactions) {
        try {
            Set<Integer> resettedMilestones = new HashSet<>();

            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(tangle, currentMilestone.getHash());
            resetSnapshotIndexOfMilestoneTransaction(milestoneTransaction, currentMilestone, resettedMilestones);
            processedTransactions.add(milestoneTransaction.getHash());

            dagHelper.traverseApprovees(
                currentMilestone.getHash(),
                currentTransaction -> currentTransaction.snapshotIndex() >= currentMilestone.index() || currentTransaction.snapshotIndex() == 0,
                currentTransaction -> resetSnapshotIndexOfMilestoneTransaction(currentTransaction, currentMilestone, resettedMilestones),
                processedTransactions
            );

            for(int resettedMilestoneIndex : resettedMilestones) {
                resetCorruptedMilestone(resettedMilestoneIndex, "resetSnapshotIndexOfMilestoneTransactions", processedTransactions);
            }
        } catch(Exception e) {
            log.error("failed to reset the transactions belonging to " + currentMilestone, e);
        }
    }

    protected void resetSnapshotIndexOfMilestoneTransaction(TransactionViewModel currentTransaction,
                                                            MilestoneViewModel currentMilestone,
                                                            Set<Integer> resettedMilestones) {
        try {
            if(currentTransaction.snapshotIndex() > currentMilestone.index()) {
                resettedMilestones.add(currentTransaction.snapshotIndex());
            }

            currentTransaction.setSnapshot(tangle, snapshotManager, 0);
        } catch(Exception e) {
            log.error("failed to reset the snapshotIndex of " + currentTransaction + " while trying to repair " + currentMilestone, e);
        }
    }

    Validity validateMilestone(SpongeFactory.Mode mode, int securityLevel, TransactionViewModel transactionViewModel, int index) throws Exception {
        if (index < 0 || index >= 0x200000) {
            return INVALID;
        }

        if (MilestoneViewModel.get(tangle, index) != null) {
            // Already validated.
            return VALID;
        }
        final List<List<TransactionViewModel>> bundleTransactions = BundleValidator.validate(tangle, snapshotManager, transactionViewModel.getHash());
        if (bundleTransactions.size() == 0) {
            return INCOMPLETE;
        }
        else {
            for (final List<TransactionViewModel> bundleTransactionViewModels : bundleTransactions) {

                final TransactionViewModel tail = bundleTransactionViewModels.get(0);
                if (tail.getHash().equals(transactionViewModel.getHash())) {
                    //the signed transaction - which references the confirmed transactions and contains
                    // the Merkle tree siblings.
                    final TransactionViewModel siblingsTx = bundleTransactionViewModels.get(securityLevel);

                    if (isMilestoneBundleStructureValid(bundleTransactionViewModels, securityLevel)) {
                        //milestones sign the normalized hash of the sibling transaction.
                        byte[] signedHash = ISS.normalizedBundle(siblingsTx.getHash().trits());

                        //validate leaf signature
                        ByteBuffer bb = ByteBuffer.allocate(Curl.HASH_LENGTH * securityLevel);
                        byte[] digest = new byte[Curl.HASH_LENGTH];

                        for (int i = 0; i < securityLevel; i++) {
                            ISSInPlace.digest(mode, signedHash, ISS.NUMBER_OF_FRAGMENT_CHUNKS * i,
                                    bundleTransactionViewModels.get(i).getSignature(), 0, digest);
                            bb.put(digest);
                        }

                        byte[] digests = bb.array();
                        byte[] address = ISS.address(mode, digests);

                        //validate Merkle path
                        byte[] merkleRoot = ISS.getMerkleRoot(mode, address,
                                siblingsTx.trits(), 0, index, numOfKeysInMilestone);
                        if ((testnet && acceptAnyTestnetCoo) || (HashFactory.ADDRESS.create(merkleRoot)).equals(coordinator)) {
                            MilestoneViewModel newMilestoneViewModel = new MilestoneViewModel(index, transactionViewModel.getHash());
                            newMilestoneViewModel.store(tangle);

                            // if we find a NEW milestone that should have been processed before our latest solid
                            // milestone -> reset the ledger state and check the milestones again
                            //
                            // NOTE: this can happen if a new subtangle becomes solid before a previous one while syncing
                            if(index < snapshotManager.getLatestSnapshot().getIndex() && index > snapshotManager.getInitialSnapshot().getIndex()) {
                                try {
                                    snapshotManager.getLatestSnapshot().rollBackMilestones(newMilestoneViewModel.index(), tangle);
                                } catch(SnapshotException e) {
                                    log.error("could not reset ledger to missing milestone: " + index);
                                }
                            }
                            return VALID;
                        } else {
                            return INVALID;
                        }
                    }
                }
            }
        }
        return INVALID;
    }

    int errorCausingMilestone = Integer.MAX_VALUE;

    int binaryBackoffCounter = 0;

    void updateLatestSolidSubtangleMilestone() throws Exception {
        // introduce some variables that help us to emit log messages while processing the milestones
        int prevSolidMilestoneIndex = snapshotManager.getLatestSnapshot().getIndex();
        long lastScan = System.currentTimeMillis();

        // get the next milestone
        MilestoneViewModel nextMilestone = MilestoneViewModel.findClosestNextMilestone(tangle, prevSolidMilestoneIndex);

        // while we have a milestone which is solid
        while(
        !shuttingDown &&
        nextMilestone != null
        ) {
            if(nextMilestone.index() > errorCausingMilestone) {
                System.out.println(errorCausingMilestone + " / " + nextMilestone.index());

                binaryBackoffCounter = 0;
                errorCausingMilestone = Integer.MAX_VALUE;
            }

            // advance to the next milestone if we were able to update the ledger state
            if (ledgerValidator.applyMilestoneToLedger(nextMilestone)) {
                if(nextMilestone.index() > latestMilestoneIndex) {
                    latestMilestoneIndex = nextMilestone.index();
                    latestMilestone = nextMilestone.getHash();
                }

                nextMilestone = MilestoneViewModel.findClosestNextMilestone(tangle, snapshotManager.getLatestSnapshot().getIndex());
            } else {
                if (TransactionViewModel.fromHash(tangle, nextMilestone.getHash()).isSolid()) {
                    int currentIndex = nextMilestone.index();
                    int targetIndex = nextMilestone.index() - binaryBackoffCounter;
                    for (int i = currentIndex; i >= targetIndex; i--) {
                        resetCorruptedMilestone(i, "updateLatestSolidSubtangleMilestone");
                    }

                    if(binaryBackoffCounter++ == 0) {
                        errorCausingMilestone = nextMilestone.index();

                        System.out.println(errorCausingMilestone + " / " + nextMilestone.index());
                    }
                }

                nextMilestone = null;
            }

            // dump a log message in intervals and when we terminate
            if(prevSolidMilestoneIndex != snapshotManager.getLatestSnapshot().getIndex() && (
            System.currentTimeMillis() - lastScan >= STATUS_LOG_INTERVAL || nextMilestone == null
            )) {
                messageQ.publish("lmsi %d %d", prevSolidMilestoneIndex, snapshotManager.getLatestSnapshot().getIndex());
                messageQ.publish("lmhs %s", snapshotManager.getLatestSnapshot().getHash());
                log.info("Latest SOLID SUBTANGLE milestone has changed from #"
                         + prevSolidMilestoneIndex + " to #"
                         + snapshotManager.getLatestSnapshot().getIndex());

                lastScan = System.currentTimeMillis();
                prevSolidMilestoneIndex = snapshotManager.getLatestSnapshot().getIndex();
            }
        }
    }

    static int getIndex(TransactionViewModel transactionViewModel) {
        return (int) Converter.longValue(transactionViewModel.trits(), TransactionViewModel.OBSOLETE_TAG_TRINARY_OFFSET, 15);
    }

    void shutDown() {
        shuttingDown = true;

        milestoneSolidifier.shutdown();
    }

    public void reportToSlack(final int milestoneIndex, final int depth, final int nextDepth) {

        try {

            final String request = "token=" + URLEncoder.encode("<botToken>", "UTF-8") + "&channel=" + URLEncoder.encode("#botbox", "UTF-8") + "&text=" + URLEncoder.encode("TESTNET: ", "UTF-8") + "&as_user=true";

            final HttpURLConnection connection = (HttpsURLConnection) (new URL("https://slack.com/api/chat.postMessage")).openConnection();
            ((HttpsURLConnection)connection).setHostnameVerifier((hostname, session) -> true);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            OutputStream out = connection.getOutputStream();
            out.write(request.getBytes("UTF-8"));
            out.close();
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            InputStream inputStream = connection.getInputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {

                result.write(buffer, 0, length);
            }
            log.info(result.toString("UTF-8"));

        } catch (final Exception e) {

            e.printStackTrace();
        }
    }

    private boolean isMilestoneBundleStructureValid(List<TransactionViewModel> bundleTxs, int securityLevel) {
        TransactionViewModel head = bundleTxs.get(securityLevel);
        return bundleTxs.stream()
                .limit(securityLevel)
                .allMatch(tx ->
                        tx.getBranchTransactionHash().equals(head.getTrunkTransactionHash()));
        //trunks of bundles are checked in Bundle validation - no need to check again.
        //bundleHash equality is checked in BundleValidator.validate() (loadTransactionsFromTangle)
    }
}
