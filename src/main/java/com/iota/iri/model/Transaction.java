package com.iota.iri.model;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Serializer;

import java.nio.ByteBuffer;

/**
 * Created by paul on 3/2/17 for iri.
 */
public class Transaction implements Persistable {
    /**
     * Size of the raw transaction bytes without the metadata.
     */
    public static final int SIZE = 1604;

    /**
     * Database schema version for this entitiy.
     *
     * It should be increased whenever we add or remove properties. The readMetadata method reacts to the version flag
     * and adjusts the parsing mechanism accordingly.
     */
    public static final byte SCHEMA_VERSION = 1;

    public byte[] bytes;

    public Hash address;
    public Hash bundle;
    public Hash trunk;
    public Hash branch;
    public Hash obsoleteTag;
    public long value;
    public long currentIndex;
    public long lastIndex;
    public long timestamp;

    public Hash tag;
    public long attachmentTimestamp;
    public long attachmentTimestampLowerBound;
    public long attachmentTimestampUpperBound;

    public int validity = 0;
    public int type = TransactionViewModel.PREFILLED_SLOT;
    public long arrivalTime = 0;

    //public boolean confirmed = false;
    public boolean parsed = false;
    public boolean solid = false;
    public long height = 0;
    public String sender = "";
    public int snapshot;
    public int referencedSnapshot = 0;

    public byte[] bytes() {
        return bytes;
    }

    public void read(byte[] bytes) {
        if(bytes != null) {
            this.bytes = new byte[SIZE];
            System.arraycopy(bytes, 0, this.bytes, 0, SIZE);
            this.type = TransactionViewModel.FILLED_SLOT;
        }
    }

    @Override
    public byte[] metadata() {
        int allocateSize =
                Hash.SIZE_IN_BYTES * 6 + //address,bundle,trunk,branch,obsoleteTag,tag
                        Long.BYTES * 9 + //value,currentIndex,lastIndex,timestamp,attachmentTimestampLowerBound,attachmentTimestampUpperBound,arrivalTime,height
                        Integer.BYTES * 4 + //validity,type,snapshot,referencedSnapshot
                        1 + //solid
                        sender.getBytes().length + //sender
                        1; // version
        ByteBuffer buffer = ByteBuffer.allocate(allocateSize);
        buffer.put(address.bytes());
        buffer.put(bundle.bytes());
        buffer.put(trunk.bytes());
        buffer.put(branch.bytes());
        buffer.put(obsoleteTag.bytes());
        buffer.put(Serializer.serialize(value));
        buffer.put(Serializer.serialize(currentIndex));
        buffer.put(Serializer.serialize(lastIndex));
        buffer.put(Serializer.serialize(timestamp));

        buffer.put(tag.bytes());
        buffer.put(Serializer.serialize(attachmentTimestamp));
        buffer.put(Serializer.serialize(attachmentTimestampLowerBound));
        buffer.put(Serializer.serialize(attachmentTimestampUpperBound));

        buffer.put(Serializer.serialize(validity));
        buffer.put(Serializer.serialize(type));
        buffer.put(Serializer.serialize(arrivalTime));
        buffer.put(Serializer.serialize(height));
        //buffer.put((byte) (confirmed ? 1:0));
        buffer.put((byte) (solid ? 1 : 0));
        buffer.put(Serializer.serialize(snapshot));
        buffer.put(Serializer.serialize(referencedSnapshot));
        buffer.put(sender.getBytes());

        // write the version flag of the transaction (introduced in version 1)
        buffer.put((byte) (SCHEMA_VERSION & 0b10000000));

        return buffer.array();
    }

    /**
     * This method parses the metadata from the saved bytes of the database.
     *
     * @param bytes metadata bytes (different length depending on schema version)
     */
    @Override
    public void readMetadata(byte[] bytes) {
        int i = 0;
        if(bytes != null) {
            // the beginning of the database structure is the same among all versions and doesn't change very often
            address = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            bundle = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            trunk = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            branch = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            obsoleteTag = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            value = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            currentIndex = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            lastIndex = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            timestamp = Serializer.getLong(bytes, i);
            i += Long.BYTES;

            tag = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            attachmentTimestamp = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            attachmentTimestampLowerBound = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            attachmentTimestampUpperBound = Serializer.getLong(bytes, i);
            i += Long.BYTES;

            validity = Serializer.getInteger(bytes, i);
            i += Integer.BYTES;
            type = Serializer.getInteger(bytes, i);
            i += Integer.BYTES;
            arrivalTime = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            height = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            /*
            confirmed = bytes[i] == 1;
            i++;
            */

            // solid flag of the transaction (if all predecessors are known)
            solid = bytes[i] == 1;
            i++;

            // the milestone index that references this transactions (0 if not confirmed yet)
            snapshot = Serializer.getInteger(bytes, i);
            i += Integer.BYTES;

            // determine the version of our schema and do some schema specific parsing
            // since the schema always ends with a string consisting out of ascii chars we can easily determine if a
            // version byte exists already by checking if the first bit of the last byte starts with a 1
            byte schemaVersion = (bytes[bytes.length - 1] & 0b10000000) != 0 ? (byte) (bytes[bytes.length - 1] & 0b01111111) : 0;
            switch (schemaVersion) {
                // schema version 1 specific properties
                case 1:
                    // the milestone that was directly or indirectly referenced by this transaction
                    referencedSnapshot = Serializer.getInteger(bytes, i);
                    i += Integer.BYTES;
            }

            // the sender bytes are always at the end because they have a dynamic size
            byte[] senderBytes = new byte[bytes.length - i];
            if (senderBytes.length != 0) {
                System.arraycopy(bytes, i, senderBytes, 0, senderBytes.length);
            }
            sender = new String(senderBytes);
            i += senderBytes.length;

            // transaction was successfully parsed from the database
            parsed = true;
        }
    }

    @Override
    public boolean merge() {
        return false;
    }
}
