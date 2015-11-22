import com.musicg.dsp.Resampler;
import com.musicg.graphic.GraphicRender;
import com.musicg.wave.Wave;
import com.musicg.wave.WaveHeader;
import com.musicg.wave.extension.Spectrogram;

public class ReadAudio {
	private static final int SAMPLE_RATE = 44100;
	
	public static double[][] extractSpectogram(String filename, boolean renderSpectrogram) {
	    Wave wave = new Wave(filename);
	    
	    Resampler resampler = new Resampler();
	    int sourceRate = wave.getWaveHeader().getSampleRate();
	    byte[] resampledBytes = resampler.reSample(wave.getBytes(), wave.getWaveHeader().getBitsPerSample(), 
	            sourceRate, SAMPLE_RATE);
	    
	    WaveHeader resampledHeader = wave.getWaveHeader();
	    resampledHeader.setSampleRate(SAMPLE_RATE);
	    Wave resampledWave = new Wave(resampledHeader, resampledBytes);
	    Spectrogram spectrogram = resampledWave.getSpectrogram();
	    	    
	    if (renderSpectrogram) {
	        GraphicRender render = new GraphicRender();
	        filename = filename.substring(filename.indexOf('/')+1);
	        render.renderSpectrogramData(spectrogram.getNormalizedSpectrogramData(), "out/"+filename+"_norm.jpg");
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
		int[] ranges = new int[] {12, 24, 36, 48, 60, 72, 100};
		for (int i = 0; i < ranges.length; i++) {
			if (value < ranges[i]) return i;
		}
		
		return ranges.length;
	}
}
