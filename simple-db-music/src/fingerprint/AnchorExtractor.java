package fingerprint;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class AnchorExtractor implements Extractor {

    private static final int TARGET_ZONE_SIZE = 5;
    private static final int TARGET_ZONE_MIN_LOOKAHEAD = 5;
    private static final int TARGET_ZONE_MAX_LOOKAHEAD = 50;
    private static final int TARGET_ZONE_DIFF = 39;
    
    @Override
    public Map<Integer, Double> matchPoints(Set<DataPoint> samplePoints,
            Map<Integer, Set<DataPoint>> knownPoints) {        
        int curHash;
        Map<Integer, Map<Integer, Integer>> songToOffsetVotes = new HashMap<>();        
        Integer curDiff;
        Integer curVotes;
        int count = 0;
        for (DataPoint dp : samplePoints) {
            curHash = dp.getHash();
            if (++count % 500 == 0) {
                System.out.println("Cur sample dp: "+count);
            }
            if (knownPoints.containsKey(curHash)) {
                for (DataPoint knownPoint : knownPoints.get(curHash)) {
                    Map<Integer, Integer> songVotes = songToOffsetVotes.get(knownPoint.getTrackId());
                    if (songVotes == null) {
                        songVotes = new HashMap<Integer, Integer>();
                        songToOffsetVotes.put(knownPoint.getTrackId(), songVotes);
                    }
                    curDiff = knownPoint.getTimeOffset() - dp.getTimeOffset();
                    curVotes = songVotes.get(curDiff);
                    if (curVotes == null) {
                        curVotes = 0;
                    }
                    curVotes += 1;
                    songVotes.put(curDiff, curVotes);
                }
            }
        }

        Map<Integer, Double> songToScore = new HashMap<>();
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
}
