import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D;

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
	
	public static double[][] fft(File audioFile) throws IOException {
		byte[] bytes = ReadAudio.toByteArray(audioFile);
		int totalSize = bytes.length;
		int numChunks = totalSize / CHUNK_SIZE;
		double[][] results = new double[numChunks][];
        double[] dub = new double[CHUNK_SIZE];
        
        DoubleFFT_1D fftDo = new DoubleFFT_1D(CHUNK_SIZE);

		for(int times = 0; times < numChunks; times++) {
			for(int i = 0; i < CHUNK_SIZE; i++) {
				//Put the time domain data into a complex number with imaginary part as 0
			    
				//complex[i] = new Complex(bytes[(times*CHUNK_SIZE)+i], 0);
			    dub[i] = bytes[(times*CHUNK_SIZE)+i];
			}	
			//Perform FFT analysis on the chunk:
			//results[times] = FFT.fft(complex);
			fftDo.realForwardFull(dub);
			results[times] = dub;
		}
		
		return results;
	}
	
}
