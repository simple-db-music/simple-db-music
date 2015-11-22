import com.musicg.dsp.Resampler;
import com.musicg.graphic.GraphicRender;
import com.musicg.wave.Wave;
import com.musicg.wave.WaveHeader;
import com.musicg.wave.extension.Spectrogram;

public class ReadAudio {
	private static final int SAMPLE_RATE = 44100;
	
	public static double[][] extractSpectogram(Wave wave, String waveName, boolean renderSpectrogram) {
	    System.out.println("length in s: "+wave.length());
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
	        render.renderSpectrogramData(spectrogram.getNormalizedSpectrogramData(), "out/"+waveName+"_norm.jpg");
	    }
	    	    
	    return spectrogram.getAbsoluteSpectrogramData();
	}
}
