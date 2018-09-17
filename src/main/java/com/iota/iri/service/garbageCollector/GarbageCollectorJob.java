package com.iota.iri.service.garbageCollector;

public abstract class GarbageCollectorJob {
    /**
     * Holds a reference to the {@link GarbageCollector} that this job belongs to.
     */
    protected GarbageCollector garbageCollector;

    /**
     * This method is used to inform the job about the {@link GarbageCollector} it belongs to.
     *
     * It automatically gets called when the jobs gets added to the {@link GarbageCollector}.
     *
     * @param garbageCollector GarbageCollector that this job belongs to
     */
    public void registerGarbageCollector(GarbageCollector garbageCollector) {
        this.garbageCollector = garbageCollector;
    }

    /**
     * This method processes the cleanup job and performs the actual pruning.
     *
     * @throws GarbageCollectorException if something goes wrong while processing the job
     */
    public abstract void process() throws GarbageCollectorException;

    /**
     * This method is used to serialize
     *
     * @return
     */
    @Override
    public abstract String toString();
}
