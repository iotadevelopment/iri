[1mdiff --git a/src/main/java/com/iota/iri/MilestoneTracker.java b/src/main/java/com/iota/iri/MilestoneTracker.java[m
[1mindex 798f52e..671dedb 100644[m
[1m--- a/src/main/java/com/iota/iri/MilestoneTracker.java[m
[1m+++ b/src/main/java/com/iota/iri/MilestoneTracker.java[m
[36m@@ -111,7 +111,8 @@[m [mpublic class MilestoneTracker {[m
                     { // Update Milestone[m
                         { // find new milestones[m
                             for(Hash hash: hashes) {[m
[31m-                                if(firstRun && (System.currentTimeMillis() - lastLogTime) >= 1000) {[m
[32m+[m[32m                                // show the scanning progress, since the first scan can potentially take a lot of time[m
[32m+[m[32m                                if(firstRun && (System.currentTimeMillis() - lastLogTime) >= 5000) {[m
                                     log.info("Scanning milestones: " + ((int) (((double) analyzedMilestoneCandidates.size() / (double) hashes.size()) * 100)) + "% done ...");[m
 [m
                                     lastLogTime = System.currentTimeMillis();[m
[36m@@ -219,7 +220,7 @@[m [mpublic class MilestoneTracker {[m
     }[m
 [m
     /**[m
[31m-     * This method allows us to hard reset the ledger state, in case we detect that  milestones were processed in the[m
[32m+[m[32m     * This method allows us to hard reset the ledger state, in case we detect that milestones were processed in the[m
      * wrong order.[m
      *[m
      * It resets the snapshotIndex of all milestones following the one provided in the parameters, removes all[m
[36m@@ -333,7 +334,7 @@[m [mpublic class MilestoneTracker {[m
     void updateLatestSolidSubtangleMilestone() throws Exception {[m
         // introduce some variables that help us to emit log messages while processing the milestones[m
         int previousSolidSubtangleLatestMilestoneIndex = snapshotManager.getLatestSnapshot().getIndex();[m
[31m-        long scanStart = System.currentTimeMillis() / 1000L;[m
[32m+[m[32m        long scanStart = System.currentTimeMillis();[m
 [m
         // get the next milestone[m
         MilestoneViewModel nextMilestone = MilestoneViewModel.findClosestNextMilestone([m
[36m@@ -352,14 +353,14 @@[m [mpublic class MilestoneTracker {[m
                 latestSolidSubtangleMilestone = nextMilestone.getHash();[m
                 snapshotManager.getLatestSnapshot().getMetaData().setIndex(nextMilestone.index());[m
 [m
[31m-                // dump a log message every second[m
[31m-                if((System.currentTimeMillis() / 1000L) - scanStart >= 1) {[m
[32m+[m[32m                // dump a log message every 5 seconds[m
[32m+[m[32m                if(System.currentTimeMillis() - scanStart >= 5000) {[m
                     messageQ.publish("lmsi %d %d", previousSolidSubtangleLatestMilestoneIndex, nextMilestone.index());[m
                     messageQ.publish("lmhs %s", latestSolidSubtangleMilestone);[m
                     log.info("Latest SOLID SUBTANGLE milestone has changed from #"[m
                              + previousSolidSubtangleLatestMilestoneIndex + " to #"[m
                              + nextMilestone.index());[m
[31m-                    scanStart = System.currentTimeMillis() / 1000L;[m
[32m+[m[32m                    scanStart = System.currentTimeMillis();[m
                     previousSolidSubtangleLatestMilestoneIndex = nextMilestone.index();[m
                 }[m
 [m
[1mdiff --git a/src/main/java/com/iota/iri/controllers/MilestoneViewModel.java b/src/main/java/com/iota/iri/controllers/MilestoneViewModel.java[m
[1mindex c9c6c01..856f9ed 100644[m
[1m--- a/src/main/java/com/iota/iri/controllers/MilestoneViewModel.java[m
[1m+++ b/src/main/java/com/iota/iri/controllers/MilestoneViewModel.java[m
[36m@@ -126,8 +126,11 @@[m [mpublic class MilestoneViewModel {[m
         // create a counter variable[m
         int currentIndex = index;[m
 [m
[32m+[m[32m        // adjust the max milestone gap according to the index (the coo ensures no gaps after milestone 650000)[m
[32m+[m[32m        int maxMilestoneGap = index >= 650000 ? 1 : MAX_MILESTONE_INDEX_GAP;[m
[32m+[m
         // try to find the next milestone by index rather than db insertion order until we are successfull[m
[31m-        while(nextMilestoneViewModel == null && ++currentIndex <= index + MAX_MILESTONE_INDEX_GAP) {[m
[32m+[m[32m        while(nextMilestoneViewModel == null && ++currentIndex <= index + maxMilestoneGap) {[m
             nextMilestoneViewModel = MilestoneViewModel.get(tangle, currentIndex);[m
         }[m
 [m
