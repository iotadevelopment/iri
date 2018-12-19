package com.iota.iri.utils.datastructure.impl;

import com.iota.iri.utils.Serializer;
import com.iota.iri.utils.datastructure.CuckooFilter;

public class DecayingCuckooFilterImpl implements CuckooFilter {
    private CuckooFilter activeFilter;

    private CuckooFilter oldFilter;

    private int itemCount;

    /**
     * Creates a cuckoo filter from a previously serialized filter.<br />
     * <br />
     * This is useful to persist the filter between restarts of the node or to transfer the state of the filter between
     * different machines.<br />
     *
     * @param serializedDecayingCuckooFilter serialized representation of the contents and parameters of the filter
     * @return in-memory representation of the filter that can be queried for membership again
     */
    public static DecayingCuckooFilterImpl unserialize(byte[] serializedDecayingCuckooFilter) {
        return new DecayingCuckooFilterImpl(serializedDecayingCuckooFilter);
    }

    public DecayingCuckooFilterImpl(int itemCount) {
        this.itemCount = itemCount;

        activeFilter = new CuckooFilterImpl(this.itemCount / 2);
    }

    private DecayingCuckooFilterImpl(byte[] serializedDecayingCuckooFilter) {
        int offset = 0;

        int filterCount = Serializer.getInteger(serializedDecayingCuckooFilter, offset);
        offset += 4;

        for (int i = 0; i < filterCount; i++) {
            int filterLength = Serializer.getInteger(serializedDecayingCuckooFilter, offset);
            offset += 4;

            byte[] serializedData = new byte[filterLength];
            System.arraycopy(serializedDecayingCuckooFilter, offset, serializedData, 0, filterLength);
            offset += filterLength;

            if (i == 0) {
                activeFilter = CuckooFilterImpl.unserialize(serializedData);
            } else {
                oldFilter = CuckooFilterImpl.unserialize(serializedData);
            }
        }
    }

    @Override
    public boolean add(byte[] item) throws IndexOutOfBoundsException {
        // if we fail to add the item to the active filter -> create a new empty one and set the active one as old
        if (!activeFilter.add(item)) {
            oldFilter = activeFilter;

            activeFilter = new CuckooFilterImpl(this.itemCount / 2);
        }

        return true;
    }

    @Override
    public boolean contains(byte[] item) {
        return (activeFilter != null && activeFilter.contains(item)) || (oldFilter != null && oldFilter.contains(item));
    }

    @Override
    public boolean delete(byte[] item) {
        if (oldFilter != null && oldFilter.contains(item)) {
            return oldFilter.delete(item);
        } else if(activeFilter != null && activeFilter.contains(item)) {
            return activeFilter.delete(item);
        }

        return false;
    }

    @Override
    public int getCapacity() {
        return 2 * activeFilter.getCapacity();
    }

    @Override
    public int size() {
        return (activeFilter != null ? activeFilter.size() : 0) + (oldFilter != null ? oldFilter.size() : 0);
    }

    @Override
    public byte[] serialize() {
        int filterCount = 0;

        byte[] activeFilterSerialized = null;
        if (activeFilter != null) {
            activeFilterSerialized = activeFilter.serialize();

            filterCount++;
        }

        byte[] oldFilterSerialized = null;
        if (oldFilter != null) {
            oldFilterSerialized = oldFilter.serialize();

            filterCount++;
        }

        int byteCount = 4 + filterCount * 4 + (activeFilterSerialized != null ? activeFilterSerialized.length : 0) +
                (oldFilterSerialized != null ? oldFilterSerialized.length : 0);

        byte[] result = new byte[byteCount];
        int offset = 0;

        System.arraycopy(Serializer.serialize(filterCount), 0, result, offset, 4);
        offset += 4;

        if (activeFilterSerialized != null) {
            System.arraycopy(Serializer.serialize(activeFilterSerialized.length), 0, result, offset, 4);
            offset += 4;

            System.arraycopy(activeFilterSerialized, 0, result, offset, activeFilterSerialized.length);
            offset += activeFilterSerialized.length;
        }

        if (oldFilterSerialized != null) {
            System.arraycopy(Serializer.serialize(oldFilterSerialized.length), 0, result, offset, 4);
            offset += 4;

            System.arraycopy(oldFilterSerialized, 0, result, offset, oldFilterSerialized.length);
        }

        return result;
    }
}
