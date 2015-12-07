package fingerprint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import simpledb.BTreeFile;
import simpledb.DbException;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;

public class RangeExtractor extends Extractor {
    
    private static final int[] FREQ_RANGES = new int[] {12, 24, 36, 48, 60, 72, 100};
    
    private final int earlyReturnThreshold;
    private final int competitorRatio;
    
    public RangeExtractor(int earlyReturnThreshold, int competitorRatio) {
        this.earlyReturnThreshold = earlyReturnThreshold;
        this.competitorRatio = competitorRatio;
    }
    
    @Override
    public Set<DataPoint> extractDataPoints(double[][] spectrogram, int trackId) {
        Set<DataPoint> dataPoints = new HashSet<DataPoint>();
        int[][] keyPoints = extractKeyPoints(spectrogram);

        int key;
        DataPoint dp;
        for (int j = keyPoints.length - 1; j > -1; j--) {
            key = Arrays.toString(keyPoints[j]).hashCode();
            dp = new DataPoint(key, j, trackId);
            dataPoints.add(dp);
        }
        
        return dataPoints;
    }


    @Override
    public Map<Integer, Double> matchPoints(Set<DataPoint> samplePoints,
            BTreeFile btree, TransactionId tid) throws NoSuchElementException, DbException, TransactionAbortedException {
        HashMap<Integer, List<Integer>> matches = new HashMap<Integer, List<Integer>>();
        HashMap<Integer, List<Integer>> times = new HashMap<Integer, List<Integer>>();
        
        Set<DataPoint> knownMatches = new HashSet<DataPoint>();
        int maxVotes = -1;
        int maxSongVotes = -1;
        int secondMostVotes = -1;
        for (DataPoint samplePoint: samplePoints) {
            knownMatches =  getPointsMatchingHash(samplePoint.getHash(), btree, tid);
            if (knownMatches.size() == 0) {
                continue;
            }
            // choose random data point from the set
            DataPoint dp = knownMatches.iterator().next();
            int songId = dp.getTrackId();
            int curNumMatches;
            if (matches.containsKey(songId) && times.containsKey(songId)) {
                matches.get(songId).add(dp.getTimeOffset());
                times.get(songId).add(samplePoint.getTimeOffset());
                curNumMatches = matches.get(songId).size();
            } else {
                List<Integer> newMatch = new ArrayList<Integer>();
                newMatch.add(dp.getTimeOffset());
                matches.put(songId, newMatch);

                List<Integer> newTime = new ArrayList<Integer>();
                newTime.add(samplePoint.getTimeOffset());
                times.put(songId, newTime);
                curNumMatches = 1;
            }
            if (curNumMatches > maxVotes) {
                if (maxSongVotes != songId) {
                    secondMostVotes = maxVotes;
                }
                maxVotes = curNumMatches;
                maxSongVotes = songId;
                if (maxVotes > earlyReturnThreshold && secondMostVotes <= maxVotes/competitorRatio) {
                    Map<Integer, Double> early = new HashMap<>();
                    // -1 is the signal that not all votes were tabulated
                    early.put(songId, -1.0);
                    return early;
                }
            }
        }
                
        Map<Integer, Double> scores = new HashMap<Integer, Double>();
        for (int songId : matches.keySet()) {
            //System.out.println("num matches for song id: "+songId+": "+matches.get(songId));
            //double percentageMatch = checkTimes(matches.get(songId), times.get(songId));
            scores.put(songId, matches.get(songId).size()*1.0);//percentageMatch);
        }
        return scores;
    }
    
    private double checkTimes(List<Integer> real, List<Integer> recorded) {
        double total = real.size();
        int correct = 0;
        
        for (int i = 1; i < real.size(); i++) {
            int realOffset = real.get(i) - real.get(i - 1);
            int recordedOffset = recorded.get(i) - recorded.get(i - 1);

            if (Math.abs(realOffset - recordedOffset) < 1) correct++;
        }
        
        return correct / total;
    }
    
    
    private int[][] extractKeyPoints(double[][] spectrogram) {
        int[][] keyPoints = new int[spectrogram.length][5];

        for (int i = 0; i < spectrogram.length; i++) {
            keyPoints[i] = new int[] {0, 12, 24, 36, 48, 60, 72, 100};
            
            for (int j = 0; j < spectrogram[i].length; j++) {
                int range = getRange(j);
                
                if (spectrogram[i][j] > spectrogram[i][keyPoints[i][range]]) {
                    keyPoints[i][range] = j;
                }
            }
            
            //System.out.println(keypoints[i][0] + " " + keypoints[i][1] + " " + keypoints[i][2] + " " + keypoints[i][3] + " " + keypoints[i][4]);
        }
        
        return keyPoints;
    }
    
    private int getRange(double value) {
        for (int i = 0; i < FREQ_RANGES.length; i++) {
            if (value < FREQ_RANGES[i]) return i;
        }
        
        return FREQ_RANGES.length;
    }

}
