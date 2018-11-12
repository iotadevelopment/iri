package com.iota.iri.service.milestone;

import com.iota.iri.model.Hash;
import com.iota.iri.utils.thread.ThreadIdentifier;
import com.iota.iri.utils.thread.ThreadUtils;

public interface MilestoneSolidifier {
    /**
     * This method allows us to add new milestones to the solidifier.<br />
     * <br />
     * Before adding the given milestone we check if it is currently relevant for our node.<br />
     *
     * @param milestoneHash Hash of the milestone that shall be solidified
     * @param milestoneIndex index of the milestone that shall be solidified
     */
    void add(Hash milestoneHash, int milestoneIndex);

    /**
     * This method starts the solidification {@link Thread} that asynchronously solidifies the milestones.
     *
     * This method is thread safe since we use a {@link ThreadIdentifier} to address the {@link Thread}. The
     * {@link ThreadUtils} take care of only launching exactly one {@link Thread} that is not terminated.
     */
    void start();

    /**
     * This method shuts down the solidification thread.
     *
     * It does not actively terminate the thread but sets the isInterrupted flag. Since we use a {@link ThreadIdentifier}
     * to address the {@link Thread}, this method is thread safe.
     */
    void shutdown();
}
