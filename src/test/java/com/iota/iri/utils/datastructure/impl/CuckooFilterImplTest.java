package com.iota.iri.utils.datastructure.impl;

import com.iota.iri.utils.datastructure.CuckooFilter;
import org.junit.*;
import org.junit.runners.MethodSorters;

/**
 * This is the Unit Test for the {@link CuckooFilterImpl}, that tests the individual methods as well as the overall
 * performance of the filter in regards to the expected false positive rate.<br />
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CuckooFilterImplTest {
    /**
     * Holds the amount of elements we want to store in the filter.<br />
     * <br />
     * Note: 1955 items allow for a ~0.955 load factor at an effective capacity of 2048<br />
     */
    private static final int ELEMENTS_TO_STORE = 1955;

    /**
     * Holds a reference to the filter that is shared throughout the tests.<br />
     */
    private CuckooFilterImpl cuckooFilter;

    /**
     * Initializes our test by creating an empty {@link CuckooFilterImpl}.<br />
     */
    @Before
    public void setUp() {
        cuckooFilter = new CuckooFilterImpl(ELEMENTS_TO_STORE);
    }

    /**
     * Frees the resources again, so the unused filter can be cleaned up by the GarbageCollector.<br />
     */
    @After
    public void teardown() {
        cuckooFilter = null;
    }

    /**
     * Tests the function of the add method by:<br />
     * <br />
     *   1. inserting the defined amount of elements<br />
     *   2. checking if the size is within the expected range<br />
     */
    @Test
    public void testAdd() {
        addTestItems(ELEMENTS_TO_STORE);

        int sizeDiff = ELEMENTS_TO_STORE - cuckooFilter.size();

        Assert.assertTrue("the filter should have less elements than we added (due to collisions)", sizeDiff >= 0);
        Assert.assertTrue("the difference in size should be less than 3%", sizeDiff <= ELEMENTS_TO_STORE * 0.03d);
    }

    /**
     * Tests the function of the contains method by checking if all previously added elements are found.<br />
     */
    @Test
    public void testContains() {
        addTestItems(ELEMENTS_TO_STORE);

        int insertedItems;
        for (insertedItems = 0; insertedItems < ELEMENTS_TO_STORE; insertedItems++) {
            Assert.assertTrue("the filter should contain all previously added elements",
                    cuckooFilter.contains(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes()));
        }
    }

    /**
     * Tests the function of the delete method by:<br />
     * <br />
     *   1. removing all previously added elements<br />
     *   2. checking if the filter is empty afterwards<br />
     */
    @Test
    public void testDelete() {
        addTestItems(ELEMENTS_TO_STORE);

        int insertedItems;
        for (insertedItems = 0; insertedItems < ELEMENTS_TO_STORE; insertedItems++) {
            cuckooFilter.delete(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes());
        }

        Assert.assertEquals("the filter should be empty", 0, cuckooFilter.size());
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
        addTestItems(ELEMENTS_TO_STORE);

        // a big enough sample size to get a reasonable result
        int elementsToQuery = 100000;

        int falsePositives = 0;
        int queriedItems;
        for (queriedItems = 0; queriedItems < elementsToQuery; queriedItems++) {
            if (cuckooFilter.contains(("QUERIED_ITEMS" + Integer.toString(queriedItems)).getBytes())) {
                falsePositives++;
            }
        }

        double falsePositiveRate = (double) falsePositives / (double) elementsToQuery;

        Assert.assertTrue("expecting the false positive rate to be lower than 3%", falsePositiveRate < 0.03d);
    }

    /**
     * Tests the function of the getCapacity method by:<br />
     * <br />
     *   1. creating filters of various sizes<br />
     *   2. comparing the created capacity against the expected range<br />
     * <br />
     * Note: Since the capacity has to be a power of two and tries to achieve a load factor of 0.955, the capacity will
     *       at max be 2.1 times the intended size.<br />
     *       <br />
     *       capacity <= 2 * (1 / 0.955) * filterSize<br />
     */
    @Test
    public void testCapacity() {
        int[] filterSizes = {10, 500, 25_000, 125_000, 10_000_000};

        CuckooFilter emptyCuckooFilter;
        for (int filterSize : filterSizes) {
            emptyCuckooFilter = new CuckooFilterImpl(filterSize);

            Assert.assertTrue("the capacity should be bigger than the intended filter size",
                    emptyCuckooFilter.getCapacity() > filterSize);

            Assert.assertTrue("the capacity should be smaller than 2.094 times the filter size",
                    emptyCuckooFilter.getCapacity() < filterSize * 2.094d);
        }
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
        addTestItems(2000);

        CuckooFilterImpl clonedCuckooFilter = CuckooFilterImpl.unserialize(cuckooFilter.serialize());

        Assert.assertEquals("the cloned filter should have the same capacity", cuckooFilter.getCapacity(),
                clonedCuckooFilter.getCapacity());
        Assert.assertEquals("the cloned filter should have the same size", cuckooFilter.size(),
                clonedCuckooFilter.size());
        for (int i = 0; i < 2000; i++) {
            Assert.assertTrue("the cloned filter should contain the previously added elements (Test" + i + ")",
                    clonedCuckooFilter.contains(("INSERTED_ITEM" + i).getBytes()));
        }
        Assert.assertFalse("the cloned filter should not contain a missing element",
                clonedCuckooFilter.contains("MISSING_ITEM".getBytes()));

    }

    private void addTestItems(int elementsToStore) {
        int insertedItems;
        for (insertedItems = 0; insertedItems < elementsToStore; insertedItems++) {
            cuckooFilter.add(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes());
        }
    }
}
