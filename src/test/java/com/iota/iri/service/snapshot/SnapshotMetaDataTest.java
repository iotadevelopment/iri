package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SnapshotMetaDataTest {
    @Test
    public void fileTest() throws IOException {
        // create some test hashes
        Hash testHash1 = new Hash("FEDKRGKR9WGSZBRLJLEMWADVPDDYURNSPQ9LKKGPHSVMGWKLLQTUUUAD9TYKCYAJLZDWXPCVBYTVSOAZY");
        Hash testHash2 = Hash.NULL_HASH;

        // create a hashset with our hashes
        HashSet<Hash> solidEntryPoints = new HashSet<>();
        solidEntryPoints.add(testHash1);
        solidEntryPoints.add(testHash2);

        // create a metadata object containing the constructed data
        SnapshotMetaData originalMetaData = new SnapshotMetaData(1337, solidEntryPoints);

        // dump our metadata file
        File metaDataFile = originalMetaData.writeFile("testMetaDataFile.msnap");

        // read the metadata file in again
        SnapshotMetaData loadedMetaData = SnapshotMetaData.fromFile(metaDataFile);

        // perform the tests on the result
        assertEquals("setIndex should be restored correctly", loadedMetaData.getIndex(), 1337);
        assertEquals("amount of solidEntryPoints should be correct", loadedMetaData.getSolidEntryPoints().size(), 2);
        assertTrue("Hash should be contained in the solidEntryPoints", loadedMetaData.hasSolidEntryPoint(testHash1));
        assertTrue("Hash should be contained in the solidEntryPoints", loadedMetaData.hasSolidEntryPoint(testHash2));

        // clean up the test file
        metaDataFile.delete();
    }
}
