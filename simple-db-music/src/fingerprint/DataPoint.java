package fingerprint;

public class DataPoint {
    
    // Whatever hash the data point represents
    // If using RangeExtractor, is a hash of the five key points in a given line of the spectrogram
    // If using AnchorExtractor, is a hash of a FrequencyPair 
    // (an (anchor freq, target freq, diff in time in "spectrogram units") tuple)
    private final int hash;
    // in "spectrogram units"
    private final int timeOffset;
    private final int trackId;

    public DataPoint(int hash, int offset, int id) {
        this.hash = hash;
        this.timeOffset = offset;
        this.trackId = id;
    }

    public int getHash() {
        return hash;
    }

    public int getTimeOffset() {
        return timeOffset;
    }

    public int getTrackId() {
        return trackId;
    }
    
    public boolean matchesHash(DataPoint otherPoint) {
        return hash == otherPoint.hash;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + hash;
        result = prime * result + timeOffset;
        result = prime * result + trackId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataPoint other = (DataPoint) obj;
        if (hash != other.hash)
            return false;
        if (timeOffset != other.timeOffset)
            return false;
        if (trackId != other.trackId)
            return false;
        return true;
    }
}
