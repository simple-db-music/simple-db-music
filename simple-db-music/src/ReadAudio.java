import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;

import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D;

public class ReadAudio {
	
	public static final int CHUNK_BYTE_SIZE = 4096;
	private static final int CHUNK_BATCH_SIZE = 1000;
//	
//	private static byte[] toByteArray(File audioFile) throws IOException, UnsupportedAudioFileException {		
//
//		int curByte = din.read();
//		List<Integer> byteList = new ArrayList<>();
//		while (curByte!= -1) {
//		    byteList.add(curByte);
//		    curByte = din.read();
//		}
//		in.close();
//		
//		byte[] bytes = new byte[byteList.size()];
//		for (int i = 0; i < bytes.length; i++) {
//		    bytes[i] = byteList.get(i).byteValue();
//		}
//		
//		return bytes;
//	}
	
	
	public static File fft(File audioFile) throws IOException, UnsupportedAudioFileException {
//		byte[] bytes = ReadAudio.toByteArray(audioFile);
//		System.out.println("Successfully read file");;

	    String fullName = audioFile.getName();
	    File f = new File(fullName.substring(0, fullName.indexOf('.'))+".fft");
	    if (f.exists()) {
	        f.delete();
	    }
	    f.createNewFile();
	    DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(f, true));
	            /*
	    try (Writer writer = new BufferedWriter(new OutputStreamWriter(
	              new FileOutputStream("filename.txt"), "utf-8"))) {
	   writer.write("something");
	}
	*/
	    
	    
	    ChunkProcessor cp = new ChunkProcessor(audioFile, CHUNK_BYTE_SIZE);
	    
        DoubleFFT_1D fftDo = new DoubleFFT_1D(CHUNK_BYTE_SIZE);
	    List<double[]> curBatch = new ArrayList<double[]>();
	    double[] curLine = new double[CHUNK_BYTE_SIZE*2];
	    List<Byte> chunkBytes = new ArrayList<Byte>();
	    int count = 0;
	    while (cp.hasChunksLeft()) {
	        curBatch.clear();
	        for(int i = 0; i < CHUNK_BATCH_SIZE; i++) {
	            count++;
	            if (count % 100 == 0) {
	                System.out.println("cur chunk: "+count);
	            }
	            if (!cp.hasChunksLeft()) {
	                break;
	            }
	            chunkBytes = cp.getNextChunk();
	            // Filter out incomplete chunks for now
	            if (chunkBytes.size() < CHUNK_BYTE_SIZE) {
	                break;
	            }
	            for (int j = 0; j < CHUNK_BYTE_SIZE; j++) {
	                curLine[j] = chunkBytes.get(j);
	            }
	            // Fill the second half of curLine with 0s - these represent complex numbers
	            for (int j = CHUNK_BYTE_SIZE; j < CHUNK_BYTE_SIZE*2; j++) {
	                curLine[i] = 0;
	            }
	            fftDo.realForwardFull(curLine);
	            curBatch.add(Arrays.copyOf(curLine, curLine.length));
	        }
	        System.out.println("writing to file...");
	        writeOutBatch(curBatch, outputStream);
	    }
	    outputStream.flush();
	    outputStream.close();
	    return f;
	    
	    /*
		double[][] results = new double[numChunks][CHUNK_SIZE*2];
        double[] dub = new double[CHUNK_SIZE*2];

        //double[] input = new double[CHUNK_SIZE*2];
        
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
		*/
	}
	
	private static void writeOutBatch(List<double[]> batch, DataOutputStream outputStream) throws IOException {
	    for (double[] line : batch) {
	        for (int i = 0; i < CHUNK_BYTE_SIZE*2; i++) {
	            outputStream.writeDouble(line[i]);
	        }
	    }
	    //outputStream.flush();
	}
	
	public static void checkFFT() {
	    double[] input = {0, 1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 0};
        DoubleFFT_1D fftDo = new DoubleFFT_1D(8);
	    fftDo.realForwardFull(input);
	    System.out.println(Arrays.toString(input));
	}
	
}
