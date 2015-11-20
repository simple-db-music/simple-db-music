import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;

import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D;

public class ReadAudio {
	
	public static final int CHUNK_SIZE = 4096;
	
	private static byte[] toByteArray(File audioFile) throws IOException, UnsupportedAudioFileException {		
		AudioInputStream in = AudioSystem.getAudioInputStream(audioFile);
		AudioInputStream din = null;
		AudioFormat baseFormat = in.getFormat();
		System.out.println("orig sample rate: "+baseFormat.getSampleRate());
		System.out.println("num channels: "+baseFormat.getChannels());
		System.out.println("orig encoding: "+baseFormat.getEncoding());
		AudioFormat decodedFormat = new AudioFormat(AudioFormat.Encoding.PCM_SIGNED, 
		                                        baseFormat.getSampleRate(),
		                                        16,
		                                        baseFormat.getChannels(),
		                                        baseFormat.getChannels() * 2,
		                                        baseFormat.getSampleRate(),
		                                        false);
		din = AudioSystem.getAudioInputStream(decodedFormat, in);
		int curByte = din.read();
		List<Integer> byteList = new ArrayList<Integer>();
		while (curByte!= -1) {
		    byteList.add(curByte);
		    curByte = din.read();
		}
		in.close();
		
		byte[] bytes = new byte[byteList.size()];
		for (int i = 0; i < bytes.length; i++) {
		    bytes[i] = byteList.get(i).byteValue();
		}
		
		return bytes;
	}
	
	public static double[][] fft(File audioFile) throws IOException, UnsupportedAudioFileException {
		byte[] bytes = ReadAudio.toByteArray(audioFile);
		System.out.println("Successfully read file");
		int totalSize = bytes.length;
		int numChunks = totalSize / CHUNK_SIZE;
		System.out.println("total size: "+totalSize);
		System.out.println("num chunks: "+numChunks);
		double[][] results = new double[numChunks][CHUNK_SIZE*2];
        double[] dub = new double[CHUNK_SIZE*2];

        //double[] input = new double[CHUNK_SIZE*2];
        
        DoubleFFT_1D fftDo = new DoubleFFT_1D(CHUNK_SIZE);
		for(int times = 0; times < numChunks; times++) {
			for(int i = 0; i < CHUNK_SIZE; i++) {
				//Put the time domain data into a complex number with imaginary part as 0
			    
				//complex[i] = new Complex(bytes[(times*CHUNK_SIZE)+i], 0);
			    dub[i] = bytes[(times*CHUNK_SIZE)+i];
			    //dub[i+CHUNK_SIZE] = dub[i];
			}
	        for (int i = CHUNK_SIZE; i < CHUNK_SIZE*2; i++) {
	            dub[i] = 0;
	        }
	        
			//Perform FFT analysis on the chunk:
			//results[times] = FFT.fft(complex);
			fftDo.realForwardFull(dub);
			//results[times] = Arrays.copyOf(dub, CHUNK_SIZE*2);
			for (int i = 0; i < CHUNK_SIZE*2; i++) {
			    results[times][i] = dub[i];
			}
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

	public static void checkFFT() {
	    double[] input = {0, 1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 0};
        DoubleFFT_1D fftDo = new DoubleFFT_1D(8);
	    fftDo.realForwardFull(input);
	    System.out.println(Arrays.toString(input));
	}
	
}
