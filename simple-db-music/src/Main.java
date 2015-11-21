import javax.sound.sampled.UnsupportedAudioFileException;

public class Main {
    
    private static final String SONG_NAME = "hi_bryan.wav";
	
	public static void main(String[] args) throws UnsupportedAudioFileException {
		try {
		    double[][] spectrogram = ReadAudio.extractSpectogram("sample_music/"+SONG_NAME, true);
		    System.out.println("num lines: "+spectrogram.length);
		    System.out.println("num freqs in line "+spectrogram[0].length);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
