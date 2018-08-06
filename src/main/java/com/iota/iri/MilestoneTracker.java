package com.iota.iri;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;

import com.iota.iri.controllers.*;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.StateDiff;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.zmq.MessageQ;
import com.iota.iri.storage.Tangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iota.iri.hash.ISS;
import com.iota.iri.model.Hash;
import com.iota.iri.utils.Converter;

import static com.iota.iri.MilestoneTracker.Validity.*;

public class MilestoneTracker {

    enum Validity {
        VALID,
        INVALID,
        INCOMPLETE
    }

    /**
     * This variable is used to keep track of the asynchronous tasks, that the "Solid Milestone Tracker" should wait for.
     */
    private AtomicInteger solidMilestoneTrackerTasks;

    private final Logger log = LoggerFactory.getLogger(MilestoneTracker.class);
    private final Tangle tangle;
    private final Hash coordinator;
    private final TransactionValidator transactionValidator;
    private final boolean testnet;
    private final MessageQ messageQ;
    private final int numOfKeysInMilestone;
    private final boolean acceptAnyTestnetCoo;
    private final SnapshotManager snapshotManager;

    private LedgerValidator ledgerValidator;
    public Hash latestMilestone = Hash.NULL_HASH;
    public Hash latestSolidSubtangleMilestone = latestMilestone;

    public int latestMilestoneIndex;

    private final Set<Hash> analyzedMilestoneCandidates = new HashSet<>();

    public MilestoneTracker(final Tangle tangle,
                            final Hash coordinator,
                            final SnapshotManager snapshotManager,
                            final TransactionValidator transactionValidator,
                            final boolean testnet,
                            final MessageQ messageQ,
                            final int numOfKeysInMilestone,
                            final boolean acceptAnyTestnetCoo
                     ) {
        this.tangle = tangle;
        this.coordinator = coordinator;
        this.snapshotManager = snapshotManager;
        this.transactionValidator = transactionValidator;
        this.testnet = testnet;
        this.messageQ = messageQ;
        this.numOfKeysInMilestone = numOfKeysInMilestone;
        this.latestMilestoneIndex = snapshotManager.getLatestSnapshot().getIndex();
        this.acceptAnyTestnetCoo = acceptAnyTestnetCoo;
    }

    private boolean shuttingDown;
    private static int RESCAN_INTERVAL = 5000;

    public void init(final SpongeFactory.Mode mode, final LedgerValidator ledgerValidator, final boolean revalidate) throws Exception {
        // to be able to process the milestones in the correct order (i.e. after a rescan of the database), we initialize
        // this variable with 1 and wait for the "Latest Milestone Tracker" to process all milestones at least once and
        // create the corresponding MilestoneViewModels to our transactions
        solidMilestoneTrackerTasks = new AtomicInteger(1);

        this.ledgerValidator = ledgerValidator;
        AtomicBoolean ledgerValidatorInitialized = new AtomicBoolean(false);
        (new Thread(() -> {
            log.info("Waiting for Ledger Validator initialization...");
            while(!ledgerValidatorInitialized.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }

            // keep track if we run the first time
            boolean firstRun = true;

            log.info("Tracker started.");
            while (!shuttingDown) {
                long scanTime = System.currentTimeMillis();

                try {
                    final int previousLatestMilestoneIndex = latestMilestoneIndex;
                    Set<Hash> hashes = AddressViewModel.load(tangle, coordinator).getHashes();
                    { // Update Milestone
                        { // find new milestones
                            for(Hash hash: hashes) {
                                if(analyzedMilestoneCandidates.add(hash)) {
                                    TransactionViewModel t = TransactionViewModel.fromHash(tangle, hash);
                                    if (t.getCurrentIndex() == 0) {
                                        final Validity valid = validateMilestone(mode, t, getIndex(t));
                                        switch (valid) {
                                            case VALID:
                                                MilestoneViewModel milestoneViewModel = MilestoneViewModel.latest(tangle);
                                                if (milestoneViewModel != null && milestoneViewModel.index() > latestMilestoneIndex) {
                                                    latestMilestone = milestoneViewModel.getHash();
                                                    latestMilestoneIndex = milestoneViewModel.index();
                                                }

                                                // mark the transaction as a snapshot
                                                t.isSnapshot(tangle, true);
                                                break;
                                            case INCOMPLETE:
                                                // issue a solidity check to solidify unsolid milestones
                                                // Note: otherwise a milestone that was followed by a coo-snapshot might
                                                //       never get solidifed again since it doesnt have connections to
                                                //       the tips
                                                transactionValidator.checkSolidity(t.getHash(), true);

                                                analyzedMilestoneCandidates.remove(t.getHash());
                                                break;
                                            case INVALID:
                                                //Do nothing
                                                break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (previousLatestMilestoneIndex != latestMilestoneIndex) {
                        messageQ.publish("lmi %d %d", previousLatestMilestoneIndex, latestMilestoneIndex);
                        log.info("Latest milestone has changed from #" + previousLatestMilestoneIndex
                                + " to #" + latestMilestoneIndex);
                    }

                    Thread.sleep(Math.max(1, RESCAN_INTERVAL - (System.currentTimeMillis() - scanTime)));
                } catch (final Exception e) {
                    log.error("Error during Latest Milestone updating", e);
                }

                // if we processed all milestone candidates once
                if(firstRun) {
                    // allow the "Solid Milestone Tracker" to continue
                    solidMilestoneTrackerTasks.decrementAndGet();

                    // only execute this part once (remember we ran once)
                    firstRun = false;
                }
            }
        }, "Latest Milestone Tracker")).start();

        (new Thread(() -> {
            log.info("Initializing Ledger Validator...");
            try {
                ledgerValidator.init();
                ledgerValidatorInitialized.set(true);
            } catch (Exception e) {
                log.error("Error initializing snapshots. Skipping.", e);
            }
            log.info("Tracker started.");
            while (!shuttingDown) {
                long scanTime = System.currentTimeMillis();

                try {
                    final int previousSolidSubtangleLatestMilestoneIndex = snapshotManager.getLatestSnapshot().getIndex();

                    if(snapshotManager.getLatestSnapshot().getIndex() < latestMilestoneIndex) {
                        updateLatestSolidSubtangleMilestone();
                    }

                    if (previousSolidSubtangleLatestMilestoneIndex != snapshotManager.getLatestSnapshot().getIndex()) {

                        messageQ.publish("lmsi %d %d", previousSolidSubtangleLatestMilestoneIndex, snapshotManager.getLatestSnapshot().getIndex());
                        messageQ.publish("lmhs %s", latestSolidSubtangleMilestone);
                        log.info("Latest SOLID SUBTANGLE milestone has changed from #"
                                + previousSolidSubtangleLatestMilestoneIndex + " to #"
                                + snapshotManager.getLatestSnapshot().getIndex());
                    }

                    Thread.sleep(Math.max(1, RESCAN_INTERVAL - (System.currentTimeMillis() - scanTime)));

                } catch (final Exception e) {
                    log.error("Error during Solid Milestone updating", e);
                }
            }
        }, "Solid Milestone Tracker")).start();


    }

    /**
     * This method allows us to reset the ledger state in case we detect, that  milestones were processed in the wrong
     * order.
     *
     * It resets the snapshotIndex of all milestones following the one provided in the parameters, removes all
     * potentially corrupt StateDiffs and restores the initial ledger state, so we can start rebuilding it. This allows
     * us to recover from the invalid ledger state without repairing or pruning the database.
     *
     * @param currentMilestone the last correct milestone
     */
    public void reset(MilestoneViewModel targetMilestone) {
        // ignore errors due to old milestones
        if(targetMilestone == null || targetMilestone.index() < snapshotManager.getInitialSnapshot().getIndex()) {
            return;
        }

        // increase a counter for the background tasks to pause the "Solid Milestone Tracker"
        solidMilestoneTrackerTasks.incrementAndGet();

        // log a message when we are resetting
        log.info("Resetting ledger to milestone " + targetMilestone.index() + " due to invalid state ...");

        // prune all potentially invalid database fields
        try {
            MilestoneViewModel currentMilestone = targetMilestone;
            while(currentMilestone != null) {
                // reset the snapshotIndex() of all following milestones to recalculate the corresponding values
                TransactionViewModel.fromHash(tangle, currentMilestone.getHash()).setSnapshot(tangle, 0);

                // remove the following StateDiffs
                tangle.delete(StateDiff.class, currentMilestone.getHash());

                // iterate to the next milestone
                currentMilestone = MilestoneViewModel.findClosestNextMilestone(
                    tangle, currentMilestone.index(), testnet, snapshotManager.getInitialSnapshot().getIndex()
                );
            }
        }

        // and inform us if sth goes wrong
        catch(Exception e) {
            // create a string representation of the stacktrace
            StringWriter stackTraceStringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceStringWriter));

            // dump the error message
            log.error("Error while resetting the ledger: " + e.getMessage() + "\n" + stackTraceStringWriter.toString());
        }

        // reset the ledger state to the initial state
        snapshotManager.resetLatestSnapshot();
        latestSolidSubtangleMilestone = Hash.NULL_HASH;

        // decrease the counter for the background tasks to unpause the "Solid Milestone Tracker"
        solidMilestoneTrackerTasks.decrementAndGet();

        // dump message when we are done
        log.info("Resetting ledger to milestone " + targetMilestone.index() + " due to invalid state ... done");
    }

    private Validity validateMilestone(SpongeFactory.Mode mode, TransactionViewModel transactionViewModel, int index) throws Exception {
        if (index < 0 || index >= 0x200000) {
            return INVALID;
        }

        if (MilestoneViewModel.get(tangle, index) != null) {
            // Already validated.
            return VALID;
        }
        final List<List<TransactionViewModel>> bundleTransactions = BundleValidator.validate(tangle, transactionViewModel.getHash());
        if (bundleTransactions.size() == 0) {
            return INCOMPLETE;
        }
        else {
            for (final List<TransactionViewModel> bundleTransactionViewModels : bundleTransactions) {

                //if (Arrays.equals(bundleTransactionViewModels.get(0).getHash(),transactionViewModel.getHash())) {
                if (bundleTransactionViewModels.get(0).getHash().equals(transactionViewModel.getHash())) {

                    //final TransactionViewModel transactionViewModel2 = StorageTransactions.instance().loadTransaction(transactionViewModel.trunkTransactionPointer);
                    final TransactionViewModel transactionViewModel2 = transactionViewModel.getTrunkTransaction(tangle);
                    if (transactionViewModel2.getType() == TransactionViewModel.FILLED_SLOT
                            && transactionViewModel.getBranchTransactionHash().equals(transactionViewModel2.getTrunkTransactionHash())
                            && transactionViewModel.getBundleHash().equals(transactionViewModel2.getBundleHash())) {

                        final byte[] trunkTransactionTrits = transactionViewModel.getTrunkTransactionHash().trits();
                        final byte[] signatureFragmentTrits = Arrays.copyOfRange(transactionViewModel.trits(), TransactionViewModel.SIGNATURE_MESSAGE_FRAGMENT_TRINARY_OFFSET, TransactionViewModel.SIGNATURE_MESSAGE_FRAGMENT_TRINARY_OFFSET + TransactionViewModel.SIGNATURE_MESSAGE_FRAGMENT_TRINARY_SIZE);

                        final byte[] merkleRoot = ISS.getMerkleRoot(mode, ISS.address(mode, ISS.digest(mode,
                                Arrays.copyOf(ISS.normalizedBundle(trunkTransactionTrits),
                                        ISS.NUMBER_OF_FRAGMENT_CHUNKS),
                                signatureFragmentTrits)),
                                transactionViewModel2.trits(), 0, index, numOfKeysInMilestone);
                        if ((testnet && acceptAnyTestnetCoo) || (new Hash(merkleRoot)).equals(coordinator)) {
                            MilestoneViewModel newMilestoneViewModel = new MilestoneViewModel(index, transactionViewModel.getHash());
                            newMilestoneViewModel.store(tangle);

                            // if we find a NEW milestone that should have been processed before our latest solid
                            // milestone -> reset the ledger state and check the milestones again
                            //
                            // NOTE: this can happen if a new subtangle becomes solid before a previous one while syncing
                            if(index < snapshotManager.getLatestSnapshot().getIndex()) {
                                reset(newMilestoneViewModel);
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

    void updateLatestSolidSubtangleMilestone() throws Exception {
        // get the next milestone
        MilestoneViewModel nextMilestone = MilestoneViewModel.findClosestNextMilestone(
            tangle, snapshotManager.getLatestSnapshot().getIndex(), testnet, snapshotManager.getInitialSnapshot().getIndex()
        );

        // while we have a milestone which is solid and which was updated + verified
        while(
            solidMilestoneTrackerTasks.get() == 0 &&
            !shuttingDown &&
            nextMilestone != null &&
            transactionValidator.checkSolidity(nextMilestone.getHash(), true)
        ) {
            if(ledgerValidator.updateSnapshot(nextMilestone)) {
                // update our internal variables
                latestSolidSubtangleMilestone = nextMilestone.getHash();
                snapshotManager.getLatestSnapshot().getMetaData().setIndex(nextMilestone.index());

                // iterate to the next milestone
                nextMilestone = MilestoneViewModel.findClosestNextMilestone(
                    tangle, snapshotManager.getLatestSnapshot().getIndex(), testnet, snapshotManager.getInitialSnapshot().getIndex()
                );
            } else {
                reset(MilestoneViewModel.get(tangle, snapshotManager.getInitialSnapshot().getIndex()));

                nextMilestone = null;
            }
        }
    }

    static int getIndex(TransactionViewModel transactionViewModel) {
        return (int) Converter.longValue(transactionViewModel.trits(), TransactionViewModel.OBSOLETE_TAG_TRINARY_OFFSET, 15);
    }

    void shutDown() {
        shuttingDown = true;
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
}