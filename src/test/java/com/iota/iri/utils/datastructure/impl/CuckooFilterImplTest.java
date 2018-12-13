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
     * Note: 1955 items allows for a ~0.955 load factor at an effective capacity of 2048<br />
     */
    private static final int ELEMENTS_TO_STORE = 1955;

    /**
     * Holds a reference to the filter that is shared throughout the tests.<br />
     */
    private static CuckooFilter cuckooFilter;

    /**
     * Initializes our test by creating an empty {@link CuckooFilterImpl}.<br />
     */
    @BeforeClass
    public static void setup() {
        cuckooFilter = new CuckooFilterImpl(ELEMENTS_TO_STORE);
    }

    /**
     * Frees the resources again, so the unused filter can be cleaned up by the GarbageCollector.<br />
     */
    @AfterClass
    public static void teardown() {
        cuckooFilter = null;
    }

    /**
     * This method tests the function of the add method by:<br />
     * <br />
     *   1. inserting the defined amount of elements<br />
     *   2. checking if the size is within the expected range<br />
     */
    @Test
    public void testAadd() {
        int insertedItems;
        for (insertedItems = 0; insertedItems < ELEMENTS_TO_STORE; insertedItems++) {
            cuckooFilter.add(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes());
        }

        int sizeDiff = ELEMENTS_TO_STORE - cuckooFilter.size();

        Assert.assertTrue("the filter should have less elements than we added (due to collisions)", sizeDiff >= 0);
        Assert.assertTrue("the difference in size should be less than 3%", sizeDiff <= ELEMENTS_TO_STORE * 0.03d);
    }

    /**
     * This method tests the function of the contains method (for the byte[] parameter) by checking if all previously
     * added elements are found.<br />
     */
    @Test
    public void testBcontains() {
        int insertedItems;
        for (insertedItems = 0; insertedItems < ELEMENTS_TO_STORE; insertedItems++) {
            Assert.assertTrue("the filter should contain all previously added elements",
                    cuckooFilter.contains(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes()));
        }
    }

    /**
     * This method tests the function of the delete method (for the byte[] parameter) by:<br />
     * <br />
     *   1. removing all previously added elements<br />
     *   2. checking if the filter is empty afterwards<br />
     */
    @Test
    public void testCdelete() {
        int insertedItems;
        for (insertedItems = 0; insertedItems < ELEMENTS_TO_STORE; insertedItems++) {
            cuckooFilter.delete(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes());
        }

        Assert.assertEquals("the filter should be empty", 0, cuckooFilter.size());


    }

    /**
     * This method tests the performance of the filter (using the byte[] parameter) in regards to false positives
     * by:<br />
     * <br />
     *   1. inserting the defined amount of elements<br />
     *   2. querying for non-existing elements<br />
     *   3. calculating the false-positive hits<br />
     *   4. comparing the value against the expected result<br />
     */
    @Test
    public void testDfalsePositiveRate() {
        int insertedItems;
        for (insertedItems = 0; insertedItems < ELEMENTS_TO_STORE; insertedItems++) {
            cuckooFilter.add(("INSERTED_ITEM" + Integer.toString(insertedItems)).getBytes());
        }

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
     * This method tests the function of the getCapacity method by:<br />
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
    public void testEcapacity() {
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

    @Test
    public void testFSerialization() {
        CuckooFilterImpl originalCuckooFilter = new CuckooFilterImpl(10000000);
        originalCuckooFilter.add("Test".getBytes());
        originalCuckooFilter.add("Test1".getBytes());

        CuckooFilterImpl clonedCuckooFilter = new CuckooFilterImpl(originalCuckooFilter.serialize());

        Assert.assertEquals("the cloned filter should have the same capacity", originalCuckooFilter.getCapacity(),
                clonedCuckooFilter.getCapacity());
        Assert.assertEquals("the cloned filter should have the same size", originalCuckooFilter.size(),
                clonedCuckooFilter.size());

        Assert.assertTrue("the cloned filter should contain the previously added elements",
                clonedCuckooFilter.contains("Test".getBytes()));
        Assert.assertTrue("the cloned filter should contain the previously added elements",
                clonedCuckooFilter.contains("Test1".getBytes()));
        Assert.assertFalse("the cloned filter should not contain a missing element",
                clonedCuckooFilter.contains("Test2".getBytes()));

    }
}
