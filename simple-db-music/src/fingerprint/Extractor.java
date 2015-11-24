package fingerprint;

import java.util.Map;
import java.util.Set;

public interface Extractor {
    Set<DataPoint> extractDataPoints(double[][] spectrogram, int trackId);
    Map<Integer, Double> matchPoints(Set<DataPoint> samplePoints, Map<Integer, Set<DataPoint>> knownPoints);
}
