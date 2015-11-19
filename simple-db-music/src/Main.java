import java.io.File;
import java.io.IOException;

public class Main {
	
	public static void main(String[] args) {
		File f = new File("sample_music/06 Melt My Heart to Stone.mp3");
		try {
			Complex[][] results = ReadAudio.fft(f);
			System.out.println(results);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
