
public class DataPoint {
    
    private final int freqHash;
    private final double timeOffset;
    private final String trackId;

    public DataPoint(int freqHash, double offset, String id) {
        this.freqHash = freqHash;
        this.timeOffset = offset;
        this.trackId = id;
    }

    public int getFreqHash() {
        return freqHash;
    }

    public double getTimeOffset() {
        return timeOffset;
    }

    public String getTrackId() {
        return trackId;
    }
    
    public boolean matchesHash(DataPoint otherPoint) {
        return freqHash == otherPoint.freqHash;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + freqHash;
        long temp;
        temp = Double.doubleToLongBits(timeOffset);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((trackId == null) ? 0 : trackId.hashCode());
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
        if (freqHash != other.freqHash)
            return false;
        if (Double.doubleToLongBits(timeOffset) != Double
                .doubleToLongBits(other.timeOffset))
            return false;
        if (trackId == null) {
            if (other.trackId != null)
                return false;
        } else if (!trackId.equals(other.trackId))
            return false;
        return true;
    }
}
