import com.musicg.dsp.Resampler;
import com.musicg.wave.Wave;
import com.musicg.wave.WaveHeader;

public class Test {

    public Test() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] args) {
        Wave wave = new Wave("known_songs/Drake - Hotline Bling.wav");
        Resampler resampler = new Resampler();
        int sourceRate = wave.getWaveHeader().getSampleRate();
        byte[] resampledBytes = resampler.reSample(wave.getBytes(), wave.getWaveHeader().getBitsPerSample(), 
                sourceRate, 44100);
        WaveHeader resampledHeader = wave.getWaveHeader();
        System.out.println("resampled bits/sample: "+resampledHeader.getBitsPerSample());
        System.out.println("channels: "+resampledHeader.getChannels());
        //System.out.println("endian: "+resampledHeader.getChannels());
    }

}
