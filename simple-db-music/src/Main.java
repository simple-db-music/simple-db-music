import java.io.File;
import java.io.IOException;

import javax.sound.sampled.UnsupportedAudioFileException;

public class Main {
	
	public static void main(String[] args) {
		File f = new File("sample_music/06 Melt My Heart to Stone.mp3");
		try {
			ReadAudio.fft2(f);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (UnsupportedAudioFileException e) {
			e.printStackTrace();
		}
	}

}
