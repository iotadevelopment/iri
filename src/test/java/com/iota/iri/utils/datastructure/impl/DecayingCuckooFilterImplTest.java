package com.iota.iri.utils.datastructure.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DecayingCuckooFilterImplTest {
    /**
     * The size of the filter we are testing.<br />
     */
    private static final int FILTER_SIZE = 1955;

    /**
     * The amount of items we add in our tests.<br />
     */
    private static final int ITEMS_TO_ADD = FILTER_SIZE * 5;

    private DecayingCuckooFilterImpl decayingCuckooFilter;

    /**
     * Initializes our test by creating an empty {@link CuckooFilterImpl}.<br />
     */
    @Before
    public void setUp() {
        decayingCuckooFilter = new DecayingCuckooFilterImpl(FILTER_SIZE);
    }

    /**
     * Frees the resources again, so the unused filter can be cleaned up by the GarbageCollector.<br />
     */
    @After
    public void teardown() {
        decayingCuckooFilter = null;
    }

    /**
     * Tests the function of the add method by inserting a number of items that far exceeds the filters capacity.<br />
     */
    @Test
    public void testAdd() {
        int insertedItems;
        for (insertedItems = 0; insertedItems < ITEMS_TO_ADD; insertedItems++) {
            Assert.assertTrue("the filter should never be full",
                    decayingCuckooFilter.add(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes()));
        }
    }

    /**
     * Tests the function of the contains method by checking if the last elements that we added are found.<br />
     */
    @Test
    public void testContains() {
        int lastItemsToCheck = 1000;

        addTestItems();

        int insertedItems;
        for (insertedItems = ITEMS_TO_ADD - lastItemsToCheck; insertedItems < ITEMS_TO_ADD; insertedItems++) {
            Assert.assertTrue("the filter should contain the last " + lastItemsToCheck + " elements",
                    decayingCuckooFilter.contains(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes()));
        }
    }

    /**
     * Tests the function of the delete method by:<br />
     * <br />
     *   1. removing all of the last elements that we added before<br />
     *   2. checking if the filter is empty afterwards<br />
     */
    @Test
    public void testDelete() {
        addTestItems();

        int insertedItems;
        for (insertedItems = ITEMS_TO_ADD - FILTER_SIZE; insertedItems <= ITEMS_TO_ADD; insertedItems++) {
            decayingCuckooFilter.delete(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes());
        }

        Assert.assertEquals("the filter should be empty", 0, decayingCuckooFilter.size());
    }

    /**
     * Tests the performance of the filter in regards to false positives by:<br />
     * <br />
     *   1. inserting a pre-defined amount of elements<br />
     *   2. querying for non-existing elements<br />
     *   3. calculating the false-positive hits<br />
     *   4. comparing the value against the expected result<br />
     */
    @Test
    public void testFalsePositiveRate() {
        addTestItems();

        // a big enough sample size to get a reasonable result
        int elementsToQuery = 100000;

        int falsePositives = 0;
        int queriedItems;
        for (queriedItems = 0; queriedItems < elementsToQuery; queriedItems++) {
            if (decayingCuckooFilter.contains(("QUERIED_ITEMS" + Integer.toString(queriedItems)).getBytes())) {
                falsePositives++;
            }
        }

        double falsePositiveRate = (double) falsePositives / (double) elementsToQuery;

        Assert.assertTrue("expecting the false positive rate to be lower than 6%", falsePositiveRate < 0.06d);
    }

    /**
     * Tests the serialization/unserialization capabilities of the filter by:<br />
     * <br />
     *   1. adding some items to our filter<br />
     *   2. serialize it<br />
     *   3. "import" the serialized data into a new filter<br />
     *   4. performing membership tests and size checks against the imported filter<br />
     */
    @Test
    public void testFSerialization() {
        addTestItems();

        DecayingCuckooFilterImpl clonedDecayingCuckooFilter = DecayingCuckooFilterImpl.unserialize(decayingCuckooFilter.serialize());

        Assert.assertEquals("the cloned filter should have the same capacity", decayingCuckooFilter.getCapacity(),
                clonedDecayingCuckooFilter.getCapacity());
        Assert.assertEquals("the cloned filter should have the same size", decayingCuckooFilter.size(),
                clonedDecayingCuckooFilter.size());

        int lastItemsToCheck = 1000;
        for (int i = ITEMS_TO_ADD - lastItemsToCheck; i < ITEMS_TO_ADD; i++) {
            Assert.assertTrue("the cloned filter should contain the previously added elements (Test" + i + ")",
                    clonedDecayingCuckooFilter.contains(("INSERTED_ITEM" + i).getBytes()));
        }
        Assert.assertFalse("the cloned filter should not contain a missing element",
                clonedDecayingCuckooFilter.contains("MISSING_ITEM".getBytes()));

    }

    private void addTestItems() {
        int insertedItems;
        for (insertedItems = 0; insertedItems < ITEMS_TO_ADD; insertedItems++) {
            decayingCuckooFilter.add(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes());
        }
    }
}
