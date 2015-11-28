package audio;
import com.musicg.dsp.Resampler;
import com.musicg.graphic.GraphicRender;
import com.musicg.wave.Wave;
import com.musicg.wave.WaveHeader;
import com.musicg.wave.extension.Spectrogram;

public class ReadAudio {
	private static final int SAMPLE_RATE = 44100;
	
    private static final int[] FREQ_RANGES = new int[] {12, 24, 36, 48, 60, 72, 100};
	
	public static double[][] extractSpectogram(Wave wave, String waveName, boolean renderSpectrogram) {
	    System.out.println("reading in song "+waveName);
	    //System.out.println("orig length in s: "+wave.length());
	    Resampler resampler = new Resampler();
	    int sourceRate = wave.getWaveHeader().getSampleRate();
	    byte[] resampledBytes = resampler.reSample(wave.getBytes(), wave.getWaveHeader().getBitsPerSample(), 
	            sourceRate, SAMPLE_RATE);
	    WaveHeader resampledHeader = wave.getWaveHeader();
	    resampledHeader.setSampleRate(SAMPLE_RATE);
	    Wave resampledWave = new Wave(resampledHeader, resampledBytes);
	    Spectrogram spectrogram = resampledWave.getSpectrogram();
	    //System.out.println("num channels: "+resampledWave.getWaveHeader().getChannels());
	    //System.out.println("resampled length in s: "+resampledWave.length());
	    if (renderSpectrogram) {
	        GraphicRender render = new GraphicRender();
	        render.renderSpectrogramData(spectrogram.getNormalizedSpectrogramData(), "out/"+waveName+"_norm.jpg");
	    }
	    	    
	    return spectrogram.getAbsoluteSpectrogramData();
	}
	
	  public static int[][] extractKeyPoints(double[][] spectrogram) {
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
	    
	    public static int getRange(double value) {
	        for (int i = 0; i < FREQ_RANGES.length; i++) {
	            if (value < FREQ_RANGES[i]) return i;
	        }
	        
	        return FREQ_RANGES.length;
	    }
	
	// Can later add methods to read in from mic in this class
}
