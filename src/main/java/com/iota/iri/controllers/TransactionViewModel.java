package com.iota.iri.controllers;

import java.util.*;

import com.iota.iri.conf.Configuration;
import com.iota.iri.model.*;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Converter;
import com.iota.iri.utils.Pair;

public class TransactionViewModel {

    private final com.iota.iri.model.Transaction transaction;

    public static final int SIZE = 1604;
    private static final int TAG_SIZE_IN_BYTES = 17; // = ceil(81 TRITS / 5 TRITS_PER_BYTE)

    public static final long SUPPLY = 2779530283277761L; // = (3^33 - 1) / 2

    public static final int SIGNATURE_MESSAGE_FRAGMENT_TRINARY_OFFSET = 0, SIGNATURE_MESSAGE_FRAGMENT_TRINARY_SIZE = 6561;
    public static final int ADDRESS_TRINARY_OFFSET = SIGNATURE_MESSAGE_FRAGMENT_TRINARY_OFFSET + SIGNATURE_MESSAGE_FRAGMENT_TRINARY_SIZE, ADDRESS_TRINARY_SIZE = 243;
    public static final int VALUE_TRINARY_OFFSET = ADDRESS_TRINARY_OFFSET + ADDRESS_TRINARY_SIZE, VALUE_TRINARY_SIZE = 81, VALUE_USABLE_TRINARY_SIZE = 33;
    public static final int OBSOLETE_TAG_TRINARY_OFFSET = VALUE_TRINARY_OFFSET + VALUE_TRINARY_SIZE, OBSOLETE_TAG_TRINARY_SIZE = 81;
    public static final int TIMESTAMP_TRINARY_OFFSET = OBSOLETE_TAG_TRINARY_OFFSET + OBSOLETE_TAG_TRINARY_SIZE, TIMESTAMP_TRINARY_SIZE = 27;
    public static final int CURRENT_INDEX_TRINARY_OFFSET = TIMESTAMP_TRINARY_OFFSET + TIMESTAMP_TRINARY_SIZE, CURRENT_INDEX_TRINARY_SIZE = 27;
    public static final int LAST_INDEX_TRINARY_OFFSET = CURRENT_INDEX_TRINARY_OFFSET + CURRENT_INDEX_TRINARY_SIZE, LAST_INDEX_TRINARY_SIZE = 27;
    public static final int BUNDLE_TRINARY_OFFSET = LAST_INDEX_TRINARY_OFFSET + LAST_INDEX_TRINARY_SIZE, BUNDLE_TRINARY_SIZE = 243;
    public static final int TRUNK_TRANSACTION_TRINARY_OFFSET = BUNDLE_TRINARY_OFFSET + BUNDLE_TRINARY_SIZE, TRUNK_TRANSACTION_TRINARY_SIZE = 243;
    public static final int BRANCH_TRANSACTION_TRINARY_OFFSET = TRUNK_TRANSACTION_TRINARY_OFFSET + TRUNK_TRANSACTION_TRINARY_SIZE, BRANCH_TRANSACTION_TRINARY_SIZE = 243;

    public static final int TAG_TRINARY_OFFSET = BRANCH_TRANSACTION_TRINARY_OFFSET + BRANCH_TRANSACTION_TRINARY_SIZE, TAG_TRINARY_SIZE = 81;
    public static final int ATTACHMENT_TIMESTAMP_TRINARY_OFFSET = TAG_TRINARY_OFFSET + TAG_TRINARY_SIZE, ATTACHMENT_TIMESTAMP_TRINARY_SIZE = 27;
    public static final int ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_OFFSET = ATTACHMENT_TIMESTAMP_TRINARY_OFFSET + ATTACHMENT_TIMESTAMP_TRINARY_SIZE, ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_SIZE = 27;
    public static final int ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_OFFSET = ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_OFFSET + ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_SIZE, ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_SIZE = 27;
    private static final int NONCE_TRINARY_OFFSET = ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_OFFSET + ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_SIZE, NONCE_TRINARY_SIZE = 81;

    public static final int TRINARY_SIZE = NONCE_TRINARY_OFFSET + NONCE_TRINARY_SIZE;

    public static final int ESSENCE_TRINARY_OFFSET = ADDRESS_TRINARY_OFFSET, ESSENCE_TRINARY_SIZE = ADDRESS_TRINARY_SIZE + VALUE_TRINARY_SIZE + OBSOLETE_TAG_TRINARY_SIZE + TIMESTAMP_TRINARY_SIZE + CURRENT_INDEX_TRINARY_SIZE + LAST_INDEX_TRINARY_SIZE;


    private AddressViewModel address;
    private ApproveeViewModel approovers;
    private TransactionViewModel trunk;
    private TransactionViewModel branch;
    private final Hash hash;


    public final static int GROUP = 0; // transactions GROUP means that's it's a non-leaf node (leafs store transaction value)
    public final static int PREFILLED_SLOT = 1; // means that we know only hash of the tx, the rest is unknown yet: only another tx references that hash
    public final static int FILLED_SLOT = -1; //  knows the hash only coz another tx references that hash

    private byte[] trits;
    public int weightMagnitude;

    public static void fillMetadata(Tangle tangle, TransactionViewModel transactionViewModel) throws Exception {
        if (Hash.NULL_HASH.equals(transactionViewModel.getHash())) {
            return;
        }
        if(transactionViewModel.getType() == FILLED_SLOT && !transactionViewModel.transaction.parsed) {
            tangle.saveBatch(transactionViewModel.getMetadataSaveBatch());
        }
    }

    public static TransactionViewModel find(Tangle tangle, byte[] hash) throws Exception {
        TransactionViewModel transactionViewModel = new TransactionViewModel((Transaction) tangle.find(Transaction.class, hash), new Hash(hash));
        fillMetadata(tangle, transactionViewModel);
        return transactionViewModel;
    }

    public static TransactionViewModel fromHash(Tangle tangle, final Hash hash) throws Exception {
        TransactionViewModel transactionViewModel = new TransactionViewModel((Transaction) tangle.load(Transaction.class, hash), hash);
        fillMetadata(tangle, transactionViewModel);
        return transactionViewModel;
    }

    public static boolean mightExist(Tangle tangle, Hash hash) throws Exception {
        return tangle.maybeHas(Transaction.class, hash);
    }

    public TransactionViewModel(final Transaction transaction, final Hash hash) {
        this.transaction = transaction == null || transaction.bytes == null ? new Transaction(): transaction;
        this.hash = hash == null? Hash.NULL_HASH: hash;
        weightMagnitude = this.hash.trailingZeros();
    }

    public TransactionViewModel(final byte[] trits, Hash hash) {
        if(trits.length == 8019) {
            transaction = new com.iota.iri.model.Transaction();
            this.trits = new byte[trits.length];
            System.arraycopy(trits, 0, this.trits, 0, trits.length);
            transaction.bytes = Converter.allocateBytesForTrits(trits.length);
            Converter.bytes(trits, 0, transaction.bytes, 0, trits.length);
            this.hash = hash;

            transaction.type = FILLED_SLOT;

            weightMagnitude = this.hash.trailingZeros();
            transaction.validity = 0;
            transaction.arrivalTime = 0;
        }
        else {
            transaction = new Transaction();
            transaction.bytes = new byte[SIZE];
            System.arraycopy(trits, 0, transaction.bytes, 0, SIZE);
            this.hash = hash;
            weightMagnitude = this.hash.trailingZeros();
            transaction.type = FILLED_SLOT;
        }
    }

    public static int getNumberOfStoredTransactions(Tangle tangle) throws Exception {
        return tangle.getCount(Transaction.class).intValue();
    }

    public boolean update(Tangle tangle, String item) throws Exception {
        getAddressHash();
        getTrunkTransactionHash();
        getBranchTransactionHash();
        getBundleHash();
        getTagValue();
        getObsoleteTagValue();
        setAttachmentData();
        setMetadata();
        if(hash.equals(Hash.NULL_HASH)) {
            return false;
        }
        return tangle.update(transaction, hash, item);
    }

    public TransactionViewModel getBranchTransaction(Tangle tangle) throws Exception {
        if(branch == null) {
            branch = TransactionViewModel.fromHash(tangle, getBranchTransactionHash());
        }
        return branch;
    }

    public TransactionViewModel getTrunkTransaction(Tangle tangle) throws Exception {
        if(trunk == null) {
            trunk = TransactionViewModel.fromHash(tangle, getTrunkTransactionHash());
        }
        return trunk;
    }

    public static byte[] trits(byte[] transactionBytes) {
        byte[] trits;
        trits = new byte[TRINARY_SIZE];
        if(transactionBytes != null) {
            Converter.getTrits(transactionBytes, trits);
        }
        return trits;
    }

    public synchronized byte[] trits() {
        return (trits == null) ? (trits = trits(transaction.bytes)) : trits;
    }

    public void delete(Tangle tangle) throws Exception {
        tangle.delete(Transaction.class, hash);
    }

    public List<Pair<Indexable, Persistable>> getMetadataSaveBatch() throws Exception {
        List<Pair<Indexable, Persistable>> hashesList = new ArrayList<>();
        hashesList.add(new Pair<>(getAddressHash(), new Address(hash)));
        hashesList.add(new Pair<>(getBundleHash(), new Bundle(hash)));
        hashesList.add(new Pair<>(getBranchTransactionHash(), new Approvee(hash)));
        hashesList.add(new Pair<>(getTrunkTransactionHash(), new Approvee(hash)));
        hashesList.add(new Pair<>(getObsoleteTagValue(), new ObsoleteTag(hash)));
        hashesList.add(new Pair<>(getTagValue(), new Tag(hash)));
        setAttachmentData();
        setMetadata();
        return hashesList;
    }

    public List<Pair<Indexable, Persistable>> getSaveBatch() throws Exception {
        List<Pair<Indexable, Persistable>> hashesList = new ArrayList<>();
        hashesList.addAll(getMetadataSaveBatch());
        getBytes();
        hashesList.add(new Pair<>(hash, transaction));
        return hashesList;
    }


    public static TransactionViewModel first(Tangle tangle) throws Exception {
        Pair<Indexable, Persistable> transactionPair = tangle.getFirst(Transaction.class, Hash.class);
        if(transactionPair != null && transactionPair.hi != null) {
            return new TransactionViewModel((Transaction) transactionPair.hi, (Hash) transactionPair.low);
        }
        return null;
    }

    public TransactionViewModel next(Tangle tangle) throws Exception {
        Pair<Indexable, Persistable> transactionPair = tangle.next(Transaction.class, hash);
        if(transactionPair != null && transactionPair.hi != null) {
            return new TransactionViewModel((Transaction) transactionPair.hi, (Hash) transactionPair.low);
        }
        return null;
    }

    public boolean store(Tangle tangle) throws Exception {
        if (hash.equals(Hash.NULL_HASH) || exists(tangle, hash)) {
            return false;
        }

        List<Pair<Indexable, Persistable>> batch = getSaveBatch();
        if (exists(tangle, hash)) {
            return false;
        }
        return tangle.saveBatch(batch);
    }

    public ApproveeViewModel getApprovers(Tangle tangle) throws Exception {
        if(approovers == null) {
            approovers = ApproveeViewModel.load(tangle, hash);
        }
        return approovers;
    }

    public final int getType() {
        return transaction.type;
    }

    public void setArrivalTime(long time) {
        transaction.arrivalTime = time;
    }

    public long getArrivalTime() {
        return transaction.arrivalTime;
    }

    public byte[] getBytes() {
        if(transaction.bytes == null || transaction.bytes.length != SIZE) {
            transaction.bytes = new byte[SIZE];
            if(trits != null) {
                Converter.bytes(trits(), 0, transaction.bytes, 0, trits().length);
            }
        }
        return transaction.bytes;
    }

    public Hash getHash() {
        return hash;
    }

    public AddressViewModel getAddress(Tangle tangle) throws Exception {
        if(address == null) {
            address = AddressViewModel.load(tangle, getAddressHash());
        }
        return address;
    }

    public TagViewModel getTag(Tangle tangle) throws Exception {
        return TagViewModel.load(tangle, getTagValue());
    }

    public Hash getAddressHash() {
        if(transaction.address == null) {
            transaction.address = new Hash(trits(), ADDRESS_TRINARY_OFFSET);
        }
        return transaction.address;
    }

    public Hash getObsoleteTagValue() {
        if(transaction.obsoleteTag == null) {
            byte[] tagBytes = Converter.allocateBytesForTrits(OBSOLETE_TAG_TRINARY_SIZE);
            Converter.bytes(trits(), OBSOLETE_TAG_TRINARY_OFFSET, tagBytes, 0, OBSOLETE_TAG_TRINARY_SIZE);

            transaction.obsoleteTag = new Hash(tagBytes, 0, TAG_SIZE_IN_BYTES);
        }
        return transaction.obsoleteTag;
    }

    public Hash getBundleHash() {
        if(transaction.bundle == null) {
            transaction.bundle = new Hash(trits(), BUNDLE_TRINARY_OFFSET);
        }
        return transaction.bundle;
    }

    public Hash getTrunkTransactionHash() {
        if(transaction.trunk == null) {
            transaction.trunk = new Hash(trits(), TRUNK_TRANSACTION_TRINARY_OFFSET);
        }
        return transaction.trunk;
    }

    public Hash getBranchTransactionHash() {
        if(transaction.branch == null) {
            transaction.branch = new Hash(trits(), BRANCH_TRANSACTION_TRINARY_OFFSET);
        }
        return transaction.branch;
    }

    public Hash getTagValue() {
        if(transaction.tag == null) {
            byte[] tagBytes = Converter.allocateBytesForTrits(TAG_TRINARY_SIZE);
            Converter.bytes(trits(), TAG_TRINARY_OFFSET, tagBytes, 0, TAG_TRINARY_SIZE);
            transaction.tag = new Hash(tagBytes, 0, TAG_SIZE_IN_BYTES);
        }
        return transaction.tag;
    }

    public long getAttachmentTimestamp() { return transaction.attachmentTimestamp; }
    public long getAttachmentTimestampLowerBound() {
        return transaction.attachmentTimestampLowerBound;
    }
    public long getAttachmentTimestampUpperBound() {
        return transaction.attachmentTimestampUpperBound;
    }


    public long value() {
        return transaction.value;
    }

    public void setValidity(Tangle tangle, int validity) throws Exception {
        if(transaction.validity != validity) {
            transaction.validity = validity;
            update(tangle, "validity");
        }
    }

    public int getValidity() {
        return transaction.validity;
    }

    public long getCurrentIndex() {
        return transaction.currentIndex;
    }

    public byte[] getSignature() {
        return Arrays.copyOfRange(trits(), SIGNATURE_MESSAGE_FRAGMENT_TRINARY_OFFSET, SIGNATURE_MESSAGE_FRAGMENT_TRINARY_SIZE);
    }

    public long getTimestamp() {
        return transaction.timestamp;
    }

    public byte[] getNonce() {
        byte[] nonce = Converter.allocateBytesForTrits(NONCE_TRINARY_SIZE);
        Converter.bytes(trits(), NONCE_TRINARY_OFFSET, nonce, 0, trits().length);
        return nonce;
    }

    public long lastIndex() {
        return transaction.lastIndex;
    }

    public void setAttachmentData() {
        getTagValue();
        transaction.attachmentTimestamp = Converter.longValue(trits(), ATTACHMENT_TIMESTAMP_TRINARY_OFFSET, ATTACHMENT_TIMESTAMP_TRINARY_SIZE);
        transaction.attachmentTimestampLowerBound = Converter.longValue(trits(), ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_OFFSET, ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_SIZE);
        transaction.attachmentTimestampUpperBound = Converter.longValue(trits(), ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_OFFSET, ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_SIZE);

    }
    public void setMetadata() {
        transaction.value = Converter.longValue(trits(), VALUE_TRINARY_OFFSET, VALUE_USABLE_TRINARY_SIZE);
        transaction.timestamp = Converter.longValue(trits(), TIMESTAMP_TRINARY_OFFSET, TIMESTAMP_TRINARY_SIZE);
        //if (transaction.timestamp > 1262304000000L ) transaction.timestamp /= 1000L;  // if > 01.01.2010 in milliseconds
        transaction.currentIndex = Converter.longValue(trits(), CURRENT_INDEX_TRINARY_OFFSET, CURRENT_INDEX_TRINARY_SIZE);
        transaction.lastIndex = Converter.longValue(trits(), LAST_INDEX_TRINARY_OFFSET, LAST_INDEX_TRINARY_SIZE);
        transaction.type = transaction.bytes == null ? TransactionViewModel.PREFILLED_SLOT : TransactionViewModel.FILLED_SLOT;
    }

    public static boolean exists(Tangle tangle, Hash hash) throws Exception {
        return tangle.exists(Transaction.class, hash);
    }

    public static Set<Indexable> getMissingTransactions(Tangle tangle) throws Exception {
        return tangle.keysWithMissingReferences(Approvee.class, Transaction.class);
    }

    public static void updateSolidTransactions(Tangle tangle, final Set<Hash> analyzedHashes) throws Exception {
        Iterator<Hash> hashIterator = analyzedHashes.iterator();
        TransactionViewModel transactionViewModel;
        while(hashIterator.hasNext()) {
            transactionViewModel = TransactionViewModel.fromHash(tangle, hashIterator.next());

            transactionViewModel.updateHeights(tangle);

            // recursively update the referenced snapshots field of the transaction
            transactionViewModel.updateReferencedSnapshot(tangle);

            if(!transactionViewModel.isSolid()) {
                transactionViewModel.updateSolid(true);
                transactionViewModel.update(tangle, "solid|height");
            }
        }
    }

    /**
     * This method updates the referencedSnapshot value of this transaction.
     *
     * It get's called once a transaction becomes solid (by updateSolidTransactions) and traverses the graph in an
     * iterative post-order way, until it finds a well defined snapshotIndex. It then updates all the found transactions
     * and stores the calculated values.
     *
     * @param tangle Tangle object which acts as a database interface
     * @throws Exception if something goes wrong while loading or storing transactions
     */
    private void updateReferencedSnapshot(Tangle tangle) throws Exception {
        // make sure we don't calculate the referencedSnapshot if we know it already
        if(referencedSnapshot() == 0) {
            try {
                // initialize our stack for graph traversal
                LinkedList<TransactionViewModel> stack = new LinkedList<TransactionViewModel>();

                // create a set of seen transactions that we do not want to traverse anymore (NULL HASH by default)
                HashSet<Hash> seenTransactions = new HashSet<Hash>(Collections.singleton(Hash.NULL_HASH));

                // add our traversal root
                seenTransactions.add(this.getHash());
                stack.push(this);

                // perform our iterative post-order traversal
                TransactionViewModel previousTransaction = null;
                while(stack.size() > 0) {
                    // retrieve the current transaction from the stack (leave it there)
                    TransactionViewModel currentTransaction = stack.peek();

                    // if we are traversing down ...
                    if(
                        previousTransaction == null ||
                        previousTransaction.getBranchTransaction(tangle) == currentTransaction ||
                        previousTransaction.getTrunkTransaction(tangle) == currentTransaction
                    ) {
                        // if we have a branch to traverse (that we haven't seen yet) -> do it ...
                        if(seenTransactions.add(currentTransaction.getBranchTransactionHash()) && !isReferencedSnapshotLeaf(currentTransaction.getBranchTransaction(tangle))) {
                            stack.push(currentTransaction.getBranchTransaction(tangle));
                        }

                        // ... or if we have a trunk to traverse (that we haven't seen yet) -> do it ...
                        else if(seenTransactions.add(currentTransaction.getTrunkTransactionHash()) && !isReferencedSnapshotLeaf(currentTransaction.getTrunkTransaction(tangle))) {
                            stack.push(currentTransaction.getTrunkTransaction(tangle));
                        }

                        // ... otherwise update the referencedSnapshot since we arrived at an end
                        else {
                            stack.pop();

                            updateReferencedSnapshotOfLeaf(tangle, currentTransaction);
                        }
                    }

                    // if we are traversing up from the branch ...
                    else if(currentTransaction.getBranchTransaction(tangle) == previousTransaction) {
                        // if we have a trunk to traverse (that we haven't seen yet) -> do it
                        if(seenTransactions.add(currentTransaction.getTrunkTransactionHash()) && !isReferencedSnapshotLeaf(currentTransaction.getTrunkTransaction(tangle))) {
                            stack.push(currentTransaction.getTrunkTransaction(tangle));
                        }

                        // otherwise -> update the referenced transaction
                        else {
                            stack.pop();

                            updateReferencedSnapshotOfLeaf(tangle, currentTransaction);
                        }
                    }

                    // if we are traversing up from the trunk -> update the referenced transaction
                    else if(currentTransaction.getTrunkTransaction(tangle) == previousTransaction) {
                        stack.pop();

                        updateReferencedSnapshotOfLeaf(tangle, currentTransaction);
                    }

                    // remember the currentTransaction to determine which way we are traversing
                    previousTransaction = currentTransaction;
                }
            } catch(ReferencedSnapshotNotProcessedYetException e) {
                /* ignore this and try again later */
            }
        }
    }

    /**
     * This method calculates the referencedSnapshot value of a transaction by examining its children.
     *
     * Since the tree traversal has already been done in the updateReferencedSnapshot method, we can only handle the
     * base cases here and not care about possible unknown values.
     *
     * It retrieves the referencedSnapshot of the branch and the trunk and saves the maximum of both in the transaction.
     *
     * @param tangle Tangle object which acts as a database interface
     * @param transaction the transactions that shall have its referencedSnapshot value calculated
     * @throws Exception if something goes wrong while loading or storing transactions or if we face a snapshot
     *                   that doesn't know it's own snapshotIndex
     */
    private void updateReferencedSnapshotOfLeaf(Tangle tangle, TransactionViewModel transaction) throws Exception {
        // retrieve the snapshotIndex of the branch
        int referencedSnapshotOfBranch;
        if(transaction.getBranchTransactionHash().equals(Hash.NULL_HASH)) {
            referencedSnapshotOfBranch = Integer.parseInt(Configuration.MAINNET_MILESTONE_START_INDEX);
        } else {
            TransactionViewModel branchTransaction = transaction.getBranchTransaction(tangle);

            if(branchTransaction.isSnapshot()) {
                if(branchTransaction.snapshotIndex() == 0) {
                    throw new ReferencedSnapshotNotProcessedYetException();
                }

                referencedSnapshotOfBranch = branchTransaction.snapshotIndex();
            } else {
                referencedSnapshotOfBranch = branchTransaction.referencedSnapshot();
            }
        }

        // retrieve the snapshotIndex of the trunk
        int referencedSnapshotOfTrunk;
        if(transaction.getTrunkTransactionHash().equals(Hash.NULL_HASH)) {
            referencedSnapshotOfTrunk = Integer.parseInt(Configuration.MAINNET_MILESTONE_START_INDEX);
        } else {
            TransactionViewModel trunkTransaction = transaction.getBranchTransaction(tangle);

            if(trunkTransaction.isSnapshot()) {
                if(trunkTransaction.snapshotIndex() == 0) {
                    throw new ReferencedSnapshotNotProcessedYetException();
                }

                referencedSnapshotOfTrunk = trunkTransaction.snapshotIndex();
            } else {
                referencedSnapshotOfTrunk = trunkTransaction.referencedSnapshot();
            }
        }

        // update the referencedSnapshot of the transaction to the bigger one of both
        transaction.referencedSnapshot(tangle, Math.max(referencedSnapshotOfBranch, referencedSnapshotOfTrunk));
    }

    /**
     * This method is a utility method that determines if we have a leaf for our graph traversal.
     *
     * A transaction is considered to be a leaf if it is either a snapshot or has a known referencedSnapshot value
     * already. We do not have to separately check for the NULL_HASH here since the seenTransactions of the
     * updateReferencedSnapshot method checks for that already.
     *
     * @param transaction transaction that shall be checked
     * @return true if we can stop traversing or false otherwise
     */
    private boolean isReferencedSnapshotLeaf(TransactionViewModel transaction) {
        return transaction.isSnapshot() || transaction.referencedSnapshot() != 0;
    }

    /**
     * This class is used to specify an error condition for our updateReferencedSnapshot method.
     *
     * It occurs if a transaction is processed in the updateReferencedSnapshot method, that refers to a milestone that
     * doesn't know its own snapshotIndex value, yet and which makes it impossible to calculate the referencedSnapshot
     * value of the child transaction. While this should theoretically not happen it is anyway better to be prepared and
     * try again later.
     */
    private class ReferencedSnapshotNotProcessedYetException extends Exception {}

    public void referencedSnapshot(Tangle tangle, final int referencedSnapshot) throws Exception {
        if ( referencedSnapshot != transaction.referencedSnapshot ) {
            transaction.referencedSnapshot = referencedSnapshot;
            update(tangle, "referencedSnapshot");
        }
    }

    public int referencedSnapshot() {
        return transaction.referencedSnapshot;
    }

    public boolean updateSolid(boolean solid) throws Exception {
        if(solid != transaction.solid) {
            transaction.solid = solid;
            return true;
        }
        return false;
    }

    public boolean isSolid() {
        return transaction.solid;
    }

    public int snapshotIndex() {
        return transaction.snapshot;
    }

    public void setSnapshot(Tangle tangle, final int index) throws Exception {
        if ( index != transaction.snapshot ) {
            transaction.snapshot = index;
            update(tangle, "snapshot");
        }
    }

    /**
     * This method is the setter for the isSnapshot flag of a transaction.
     *
     * It gets automatically called by the "Latest Milestone Tracker" and marks transactions that represent a milestone
     * accordingly. It first checks if the value has actually changed and then issues a database update.
     *
     * @param tangle Tangle instance which acts as a database interface
     * @param isSnapshot true if the transaction is a snapshot and false otherwise
     * @throws Exception if something goes wrong while saving the changes to the database
     */
    public void isSnapshot(Tangle tangle, final boolean isSnapshot) throws Exception {
        if ( isSnapshot != transaction.isSnapshot ) {
            transaction.isSnapshot = isSnapshot;
            update(tangle, "isSnapshot");
        }
    }

    /**
     * This method is the getter for the isSnapshot flag of a transaction.
     *
     * The isSnapshot flag indicates if the transaction is a coordinator issued milestone. It allows us to differentiate
     * the two types of transactions (normal transactions / milestones) very fast and efficiently without issuing
     * further database queries or even full verifications of the signature. If it is set to true one can for example
     * use the snapshotIndex() method to retrieve the corresponding MilestoneViewModel object.
     *
     * @return true if the transaction is a milestone and false otherwise
     */
    public boolean isSnapshot() {
        return transaction.isSnapshot;
    }

    public long getHeight() {
        return transaction.height;
    }

    private void updateHeight(long height) throws Exception {
        transaction.height = height;
    }

    public void updateHeights(Tangle tangle) throws Exception {
        TransactionViewModel transactionVM = this, trunk = this.getTrunkTransaction(tangle);
        Stack<Hash> transactionViewModels = new Stack<>();
        transactionViewModels.push(transactionVM.getHash());
        while(trunk.getHeight() == 0 && trunk.getType() != PREFILLED_SLOT && !trunk.getHash().equals(Hash.NULL_HASH)) {
            transactionVM = trunk;
            trunk = transactionVM.getTrunkTransaction(tangle);
            transactionViewModels.push(transactionVM.getHash());
        }
        while(transactionViewModels.size() != 0) {
            transactionVM = TransactionViewModel.fromHash(tangle, transactionViewModels.pop());
            long currentHeight = transactionVM.getHeight();
            if(Hash.NULL_HASH.equals(trunk.getHash()) && trunk.getHeight() == 0
                    && !Hash.NULL_HASH.equals(transactionVM.getHash())) {
                if(currentHeight != 1L ){
                    transactionVM.updateHeight(1L);
                    transactionVM.update(tangle, "height");
                }
            } else if ( trunk.getType() != PREFILLED_SLOT && transactionVM.getHeight() == 0){
                long newHeight = 1L + trunk.getHeight();
                if(currentHeight != newHeight) {
                    transactionVM.updateHeight(newHeight);
                    transactionVM.update(tangle, "height");
                }
            } else {
                break;
            }
            trunk = transactionVM;
        }
    }

    public void updateSender(String sender) throws Exception {
        transaction.sender = sender;
    }
    public String getSender() {
        return transaction.sender;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionViewModel other = (TransactionViewModel) o;
        return Objects.equals(getHash(), other.getHash());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getHash());
    }
}
