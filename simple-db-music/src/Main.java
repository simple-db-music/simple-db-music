import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sound.sampled.UnsupportedAudioFileException;

import com.musicg.wave.Wave;

public class Main {

    private static final String SONG_NAME = "1-07 Can't Feel My Face.wav";//"hi_bryan.wav";
    private static final int TARGET_ZONE_SIZE = 10;
    private static final int TARGET_ZONE_MIN_LOOKAHEAD = 5;
    private static final int TARGET_ZONE_MAX_LOOKAHEAD = 100;
    private static final int TARGET_ZONE_DIFF = 39;
    
    private static final String[] KNOWN_SONGS = {"1-07 Can't Feel My Face.wav",
            "Drake - Hotline Bling.wav"
    };
    
    private static final String SAMPLE_SONG = "can't feel my face sample.wav";

    public static void main(String[] args) throws UnsupportedAudioFileException {
        try {
            
            Set<DataPoint> knownDataPoints = new HashSet<>();
            for (String song : KNOWN_SONGS) {
                knownDataPoints.addAll(getDataPoints(song));
            }
            
            Set<DataPoint> sampleDataPoints = getDataPoints(SAMPLE_SONG);
            
            match(sampleDataPoints, knownDataPoints);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void match(Set<DataPoint> samplePoints, Set<DataPoint> knownPoints) {
        Map<Integer, Set<DataPoint>> hashToPoints = getHashToPoints(knownPoints);
        
        Map<String, Set<TimeOffsetPair>> songToTimes = new HashMap<>();
        int curHash;
        String curTrack;
        TimeOffsetPair curOffsets;
        Set<TimeOffsetPair> offsetSet;
        int count = 0;
        for (DataPoint dp : samplePoints) {
            curHash = dp.getFreqHash();
            if (count++ % 500 == 0) {
                System.out.println("Cur sample dp: "+count);
            }
            if (hashToPoints.containsKey(curHash)) {
                for (DataPoint knownPoint : hashToPoints.get(curHash)) {
                    curOffsets = new TimeOffsetPair(dp.getTimeOffset(), knownPoint.getTimeOffset());
                    curTrack = knownPoint.getTrackId();
                    if (songToTimes.containsKey(curTrack)) {
                        offsetSet = songToTimes.get(curTrack);
                    } else {
                        offsetSet = new HashSet<>();
                    }
                    offsetSet.add(curOffsets);
                    songToTimes.put(curTrack, offsetSet);
                }
            }
        }
        
        System.out.println("Key set: "+songToTimes.keySet().toString());
        
        for (String song : KNOWN_SONGS) {
            System.out.println("Num matches for "+song+": "+songToTimes.get(song));
        }
        
        Map<String, Integer> songToScore = new HashMap<>();
        Map<Double, Integer> offsetVotes;
        Double offsetDiff;
        Integer votes;
        for (String song : songToTimes.keySet()) {
            offsetSet = songToTimes.get(song);
            offsetVotes = new HashMap<>();
            for(TimeOffsetPair offset : offsetSet) {
                offsetDiff = offset.getRoundedOffsetDifference();
                if (offsetVotes.containsKey(offsetDiff)) {
                    votes = offsetVotes.get(offsetDiff);
                } else {
                    votes = 0;
                }
                votes += 1;
                offsetVotes.put(offsetDiff, votes);
            }
            songToScore.put(song, offsetVotes.values().stream().max(Integer::compare).get());
        }
        
        System.out.println("Scores: "+songToScore);
        
    }
    
    public static Map<Integer, Set<DataPoint>> getHashToPoints(Set<DataPoint> knownPoints) {
        Map<Integer, Set<DataPoint>> hashToPoints = new HashMap<>();
        int curHash;
        Set<DataPoint> points;
        for (DataPoint dp : knownPoints) {
            curHash = dp.getFreqHash();
            if (hashToPoints.containsKey(curHash)) {
                points = hashToPoints.get(curHash);
            } else {
                points = new HashSet<>();
            }
            points.add(dp);
            hashToPoints.put(curHash, points);
        }
        
        int count = 0;
        for (Set<DataPoint> s : hashToPoints.values()) {
            count += s.size();
        }
        
        System.out.println("Avg num of colliding hashes: "+count*1.0/hashToPoints.keySet().size());
        
        return hashToPoints;
    }

    public static int[] extractKeyPoints(double[][] spectrogram) {
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
    
    public static Set<DataPoint> getDataPoints(String songName) {
        Wave wave = new Wave("sample_music/"+songName);
        double[][] spectrogram = ReadAudio.extractSpectogram(wave, songName, false);
        System.out.println(songName+" num lines: "+spectrogram.length);

        int[] keyPoints = extractKeyPoints(spectrogram);
        List<DataPoint> dataPoints = getDataPoints(keyPoints, wave.length()*1.0/spectrogram.length);
        System.out.println(songName+" num data points (list): "+dataPoints.size());
        Set<DataPoint> points = new HashSet<DataPoint>(dataPoints);
        System.out.println(songName+" num data points (set): "+points.size());
        return points;
    }

    public static List<DataPoint> getDataPoints(int[] keyPoints, double lineDuration) {
        List<DataPoint> dataPoints = new ArrayList<DataPoint>();
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
                curPair = new FrequencyPair(keyPoints[i], keyPoints[j], (j-i)*lineDuration);
                dataPoints.add(new DataPoint(curPair.hashCode(), lineDuration*i, SONG_NAME));

                if (curPointsInTargetZone == TARGET_ZONE_SIZE) {
                    break;
                }
            }
        }    
        return dataPoints;
    }

}
