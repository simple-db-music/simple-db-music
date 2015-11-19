import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ReadAudio {
	
	private static final int CHUNK_SIZE = 4096;
	
	private static byte[] toByteArray(File audioFile) throws IOException {
		FileInputStream inputStream = new FileInputStream(audioFile);
		int length = (int) audioFile.length();
		byte[] bytes = new byte[length];
		for (int i=0; i < length; i++) {
			bytes[i] = (byte) inputStream.read();
		}
		return bytes;
	}
	
	public static Complex[][] fft(File audioFile) throws IOException {
		byte[] bytes = ReadAudio.toByteArray(audioFile);
		int totalSize = bytes.length;
		int numChunks = totalSize / CHUNK_SIZE;
		Complex[][] results = new Complex[numChunks][];
		
		for(int times = 0; times < numChunks; times++) {
			Complex[] complex = new Complex[CHUNK_SIZE];
			for(int i = 0; i < CHUNK_SIZE; i++) {
				//Put the time domain data into a complex number with imaginary part as 0
				complex[i] = new Complex(bytes[(times*CHUNK_SIZE)+i], 0);
			}	
			//Perform FFT analysis on the chunk:
			results[times] = FFT.fft(complex);
		}
		
		return results;
	}
	
}
