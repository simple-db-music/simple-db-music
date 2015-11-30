package fingerprint;
public class FrequencyPair {

    private final int anchorFrequency;
    private final int targetFrequency;
    // in "spectrogram units"
    private final int timeDiff;
    
    public FrequencyPair(int anchorFrequency, int targetFrequency, int timeDiff) {
        this.anchorFrequency = anchorFrequency;
        this.targetFrequency = targetFrequency;
        this.timeDiff = timeDiff;
    } 

    public int getAnchorFrequency() {
        return anchorFrequency;
    }

    public int getTargetFrequency() {
        return targetFrequency;
    }
    
    public int getTimeDifference() {
        return timeDiff;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + anchorFrequency;
        result = prime * result + targetFrequency;
        result = prime * result + timeDiff;
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
        FrequencyPair other = (FrequencyPair) obj;
        if (anchorFrequency != other.anchorFrequency)
            return false;
        if (targetFrequency != other.targetFrequency)
            return false;
        if (timeDiff != other.timeDiff)
            return false;
        return true;
    }
}