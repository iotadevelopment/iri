package com.iota.iri.service.snapshot;

public class GarbageCollectorJob {
    private int startingIndex;

    private int currentIndex;

    public GarbageCollectorJob(int startingIndex, int currentIndex) {
        this.startingIndex = startingIndex;
        this.currentIndex = currentIndex;
    }

    public int getCurrentIndex() {
        return currentIndex;
    }

    public int getStartingIndex() {
        return startingIndex;
    }

    public void setCurrentIndex(int currentIndex) {
        this.currentIndex = currentIndex;
    }

    public void setStartingIndex(int startingIndex) {
        this.startingIndex = startingIndex;
    }
}
