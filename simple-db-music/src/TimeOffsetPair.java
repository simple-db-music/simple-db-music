import java.text.DecimalFormat;

public class TimeOffsetPair {

    private final double sampleOffset;
    private final double knownOffset;
    private static final DecimalFormat FORMATTER = new DecimalFormat("#,##0.000");
    
    public TimeOffsetPair(double sampleOffset, double knownOffset) {
        this.sampleOffset = sampleOffset;
        this.knownOffset = knownOffset;
    }
    
    public double getSampleOffset() {
        return sampleOffset;
    }

    public double getKnownOffset() {
        return knownOffset;
    }
    
    public double getRoundedOffsetDifference() {
        return Double.valueOf(FORMATTER.format(knownOffset - sampleOffset));
    }

}