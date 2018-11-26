package com.iota.iri.service.milestone.impl;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.persistables.Milestone;
import com.iota.iri.model.persistables.Transaction;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.service.snapshot.impl.SnapshotImpl;
import com.iota.iri.service.snapshot.impl.SnapshotProviderImpl;
import com.iota.iri.service.snapshot.impl.SnapshotServiceImpl;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Pair;
import com.iota.iri.zmq.MessageQ;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MilestoneServiceImplTest {
    private MilestoneServiceImpl milestoneService;

    @Mock
    private SnapshotProvider snapshotProvider;

    @Mock
    private SnapshotService snapshotService;

    @Mock
    private Tangle tangle;

    @Mock
    private MessageQ messageQ;

    @Mock
    private Snapshot initialSnapshot;

    @Before
    public void setup() throws SnapshotException {
        MainnetConfig config = new MainnetConfig();

        milestoneService = new MilestoneServiceImpl().init(tangle, snapshotProvider, snapshotService, messageQ, config);
    }

    private Milestone mockMilestone(String hash, int index, boolean applied) throws Exception {
        Milestone latestMilestone = new Milestone();
        latestMilestone.hash = HashFactory.TRANSACTION.create(hash);
        latestMilestone.index = new IntegerIndex(index);

        Mockito.when(tangle.load(Milestone.class, new IntegerIndex(index))).thenReturn(latestMilestone);

        Transaction latestMilestoneTransaction = new Transaction();
        latestMilestoneTransaction.bytes = new byte[0];
        latestMilestoneTransaction.type = TransactionViewModel.FILLED_SLOT;
        latestMilestoneTransaction.snapshot = applied ? index : 0;
        latestMilestoneTransaction.parsed = true;

        Mockito.when(tangle.load(Transaction.class, latestMilestone.hash)).thenReturn(latestMilestoneTransaction);

        return latestMilestone;
    }

    @Test
    public void findLatestProcessedSolidMilestoneInDatabase() throws Exception {
        mockSnapshotProvider();

        ;

        // mock tangle
        mockMilestone("ARWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 1, true);
        mockMilestone("BRWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 2, false);
        mockMilestone("CRWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 3, false);
        mockMilestone("DRWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 4, false);
        mockMilestone("ERWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 5, false);
        mockMilestone("GRWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 7, false);
        mockMilestone("IRWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 9, false);
        mockMilestone("JRWY9LWHXEWNL9DTN9IGMIMIVSBQUIEIDSFRYTCSXQARRTVEUFSBWFZRQOJUQNAGQLWHTFNVECELCOFYB", 10, false);
        mockMilestone("HZYNIHQQTMOPVVSIHGWENIVLJNIODSQBFEW9WUUFIP9BIFBXVLVGZLIZMQBEFHOOZBPVQJLKLGWVA9999", 11, false);
        Milestone latestMilestone = mockMilestone("SVARYFXHBTZQYGEEUKOOOOE9IRNNUMKJPLLCBSJBCCXNRG9WKKUPQQQLKYWWBOQTAJYNZMI9AY9RZ9999", 11002, false);

        Mockito.when(tangle.getLatest(Milestone.class, IntegerIndex.class)).thenReturn(new Pair<>(latestMilestone.index, latestMilestone));

        System.out.println(milestoneService.findLatestProcessedSolidMilestoneInDatabase());
    }

    private void mockSnapshotProvider() {
        Mockito.when(initialSnapshot.getIndex()).thenReturn(0);

        Mockito.when(snapshotProvider.getInitialSnapshot()).thenReturn(initialSnapshot);
    }
}
