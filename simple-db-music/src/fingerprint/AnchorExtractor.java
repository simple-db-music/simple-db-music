package fingerprint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import simpledb.BTreeFile;
import simpledb.DbException;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;

public class AnchorExtractor extends Extractor {

    private static final int TARGET_ZONE_SIZE = 5;
    private static final int TARGET_ZONE_MIN_LOOKAHEAD = 5;
    private static final int TARGET_ZONE_MAX_LOOKAHEAD = 50;
    private static final int TARGET_ZONE_DIFF = 39;
    
    private static final int RAND_SAMPLE_SIZE = 250;
    
    @Override
    public Map<Integer, Double> matchPoints(Set<DataPoint> samplePoints,
           BTreeFile btree, TransactionId tid) throws NoSuchElementException, DbException, TransactionAbortedException {        
        Map<Integer, Map<Integer, Integer>> songToOffsetVotes = new HashMap<Integer, Map<Integer, Integer>>();        
        //System.out.println("sorting sample points...");
        List<DataPoint> sampleList = new ArrayList<DataPoint>(samplePoints);
        //Collections.sort(sortedSamplePoints, (p1, p2) -> p1.getHash() - p2.getHash());;
        //System.out.println("done!");
        int maxVotes = -1;
        int maxSong = -1;
        int maxVotes2 = -1;
        int maxSong2 = -1;
        int count = 0;
        //int totalMatching = 0;
      // for (DataPoint dp : randomSample(sampleList)) {
        for (DataPoint dp : sampleList) {
        //randomSample(sampleList).parallelStream().forEach(dp -> {
            int curHash = dp.getHash();
            if (++count % 500 == 0) {
                System.out.println("Cur sample dp: "+count);
            }
            Set<DataPoint> knownPoints;
            try {
                knownPoints = getPointsMatchingHash(curHash, btree, tid);
              //totalMatching += knownPoints.size();
                //System.out.println("num points matched to: "+knownPoints.size());
                for (DataPoint knownPoint : knownPoints) {
                    /*
                    int trackId = knownPoint.getTrackId();
                    if (trackId < 16) {
                        System.out.println("matched to song with track id "+trackId);
                    }
                    */
                    Map<Integer, Integer> songVotes = songToOffsetVotes.get(knownPoint.getTrackId());
                    if (songVotes == null) {
                        songVotes = new HashMap<Integer, Integer>();
                        songToOffsetVotes.put(knownPoint.getTrackId(), songVotes);
                    }
                    Integer curDiff = knownPoint.getTimeOffset() - dp.getTimeOffset();
                    Integer curVotes = songVotes.get(curDiff);
                    if (curVotes == null) {
                        curVotes = 0;
                    }
                    curVotes += 1;
                    songVotes.put(curDiff, curVotes);
                    /*
                    if (curVotes > maxVotes) {
                        if (maxSong != knownPoint.getTrackId()) {
                            maxSong2 = maxSong;
                            maxVotes2 = maxVotes;
                        }
                        maxVotes = curVotes;
                        maxSong = knownPoint.getTrackId();
                        if (maxVotes > 20 && maxVotes2 <= maxVotes/2) {
                            Map<Integer, Double> early = new HashMap<>();
                            early.put(knownPoint.getTrackId(), -1.0);
                            return early;
                        }
                    }
                    */
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }//);

        //System.out.println("avg matches per point: "+1.0*totalMatching/samplePoints.size());
        
        Map<Integer, Double> songToScore = new HashMap<Integer, Double>();
        for (Integer songId : songToOffsetVotes.keySet()) {
            Map<Integer, Integer> votes = songToOffsetVotes.get(songId);
            int max = -1;
            //double maxDiff = -1;
            for (Entry<Integer, Integer> e: votes.entrySet()) {
                if (max < e.getValue()) {
                    max = e.getValue();
                    //maxDiff = e.getKey();
                }
            }
            //System.out.println("max diff for "+song+": "+maxDiff);
            songToScore.put(songId, (double) max);
        }

        return songToScore;
    }
    
    @Override
    public Set<DataPoint> extractDataPoints(double[][] spectrogram, int trackId) {
        int[] keyPoints = extractKeyPoints(spectrogram);
        Set<DataPoint> dataPoints = new HashSet<DataPoint>();
        int upperBound;
        int curPointsInTargetZone;
        FrequencyPair curPair;
        for (int i = 0; i < keyPoints.length; i++) {
            upperBound = Math.min(i+TARGET_ZONE_MAX_LOOKAHEAD, keyPoints.length);
            curPointsInTargetZone = 0;

            for (int j = i+TARGET_ZONE_MIN_LOOKAHEAD; j < upperBound; j++) {
                if (Math.abs(keyPoints[i]-keyPoints[j]) > TARGET_ZONE_DIFF) {
                    continue;
                }

                curPointsInTargetZone++;
                curPair = new FrequencyPair(keyPoints[i], keyPoints[j], j-i);
                dataPoints.add(new DataPoint(curPair.hashCode(), i, trackId));

                if (curPointsInTargetZone == TARGET_ZONE_SIZE) {
                    break;
                }
            }
        }    
        return dataPoints;
    }
    
    private int[] extractKeyPoints(double[][] spectrogram) {
        int[] keyPoints = new int[spectrogram.length];
        double maxAmplitude;
        int maxFrequency;
        double[] curLine;
        for (int i = 0; i < spectrogram.length; i++) {
            maxAmplitude = Integer.MIN_VALUE;
            maxFrequency = -1;
            curLine = spectrogram[i];
            for (int j = 0; j < curLine.length; j++) {
                if (curLine[j] > maxAmplitude) {
                    maxAmplitude = curLine[j];
                    maxFrequency = j;
                }
            }
            keyPoints[i] = maxFrequency;
        }

        return keyPoints;
    }
    
    private List<DataPoint> randomSample(List<DataPoint> points) {
        Random r = new Random();
        for (int i = 0; i < RAND_SAMPLE_SIZE; i++) {
            int pos = i + r.nextInt(points.size() - i);
            DataPoint temp = points.get(pos);
            points.set(pos, points.get(i));
            points.set(i, temp);
        }
        return points.subList(0, RAND_SAMPLE_SIZE);
    }
}
