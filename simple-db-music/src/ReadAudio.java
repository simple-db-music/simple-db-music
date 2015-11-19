import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;

public class ReadAudio {
	
	private static final int CHUNK_SIZE = 4096;
	
	private static byte[] toByteArray(File audioFile) throws UnsupportedAudioFileException, IOException {
		
		// FileInputStream din = new FileInputStream(audioFile);
		
		AudioInputStream in= AudioSystem.getAudioInputStream(audioFile);
		AudioInputStream din = null;
		AudioFormat baseFormat = in.getFormat();
		AudioFormat decodedFormat = new AudioFormat(AudioFormat.Encoding.PCM_SIGNED, 
		                                            baseFormat.getSampleRate(),
		                                            16,
		                                            baseFormat.getChannels(),
		                                            baseFormat.getChannels() * 2,
		                                            baseFormat.getSampleRate(),
		                                            false);
		din = AudioSystem.getAudioInputStream(decodedFormat, in);
		
		int length = (int) audioFile.length();
		byte[] bytes = new byte[length];
		for (int i=0; i < length; i++) {
			bytes[i] = (byte) din.read();
		}
		return bytes;
	}
	
	public static Complex[][] fft1(File audioFile) throws IOException, UnsupportedAudioFileException {
		byte[] bytes = ReadAudio.toByteArray(audioFile);
		System.out.println("Successfully read file");
		int totalSize = bytes.length;
		int numChunks = totalSize / CHUNK_SIZE;
		Complex[][] results = new Complex[numChunks][];
		Complex[] complex = new Complex[CHUNK_SIZE];
		for(int times = 0; times < numChunks; times++) {
			for(int i = 0; i < CHUNK_SIZE; i++) {
				//Put the time domain data into a complex number with imaginary part as 0
				complex[i] = new Complex(bytes[(times*CHUNK_SIZE)+i], 0);
			}	
			//Perform FFT analysis on the chunk:
			results[times] = FFT.fft(complex);
			System.out.println("FFT " + times);
		}
		
		return results;
	}
	
	public static void fft2(File audioFile) throws IOException, UnsupportedAudioFileException {
		byte[] bytes = ReadAudio.toByteArray(audioFile);
		System.out.println("Successfully read file");
		int totalSize = bytes.length;
		int numChunks = totalSize / CHUNK_SIZE;
		double[][] audioRe = new double[numChunks][];
		double[][] audioIm = new double[numChunks][];
		double[][] audioMag = new double[numChunks][];
		FFT2 fft = new FFT2(CHUNK_SIZE);
		double[] real = new double[CHUNK_SIZE];
		double[] imaginary =  new double[CHUNK_SIZE];
		for(int times = 0; times < numChunks; times++) {
			for(int i = 0; i < CHUNK_SIZE; i++) {
				//Put the time domain data into a complex number with imaginary part as 0
				real[i] = bytes[(times*CHUNK_SIZE)+i];
				imaginary[i] = 0;
			}
			fft.fft(real, imaginary);
			audioRe[times] =  real;
			audioIm[times] = imaginary;
			double[] magnitudes = new double[CHUNK_SIZE];
			for (int i = 0; i < CHUNK_SIZE; i++) {
				magnitudes[i] = Math.sqrt(real[i] * real[i] + imaginary[i] * imaginary[i]);
			}
			audioMag[times] = magnitudes;
		}
		System.out.println("Finished FFT");
	}
	
}
