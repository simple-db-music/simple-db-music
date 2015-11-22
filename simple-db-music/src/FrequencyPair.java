import java.text.DecimalFormat;

public class FrequencyPair {

    private final int anchorFrequency;
    private final int targetFrequency;
    private final double timeDiff;
    
    public FrequencyPair(int anchorFrequency, int targetFrequency, double timeDiff) {
        this.anchorFrequency = anchorFrequency;
        this.targetFrequency = targetFrequency;
        DecimalFormat df = new DecimalFormat("#,##0.000");
        this.timeDiff = Double.valueOf(df.format(timeDiff));
        //this.timeDiff = timeDiff;
    } 

    public int getAnchorFrequency() {
        return anchorFrequency;
    }

    public int getTargetFrequency() {
        return targetFrequency;
    }
    
    public double getTimeDifference() {
        return timeDiff;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + anchorFrequency;
        result = prime * result + targetFrequency;
        long temp;
        temp = Double.doubleToLongBits(timeDiff);
        result = prime * result + (int) (temp ^ (temp >>> 32));
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
        if (Double.doubleToLongBits(timeDiff) != Double
                .doubleToLongBits(other.timeDiff))
            return false;
        return true;
    }
}
