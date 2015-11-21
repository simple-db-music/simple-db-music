import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import javax.sound.sampled.UnsupportedAudioFileException;

import visual.Spectrogram;

public class Main {
	
	public static void main(String[] args) throws UnsupportedAudioFileException {
		//File f = new File("sample_music/06 Melt My Heart To Stone.mp3");
	    //File f = new File("sample_music/00 clique_mp3.mp3");
	    File f = new File("sample_music/00 Requiem for a dream Instrumental_mp3.mp3");
		//ReadAudio.checkFFT();
	    
		try {
		    //ChunkProcessor c = new ChunkProcessor(f, 4096);
		    //File createdFile = ReadAudio.fft(f);
		    File createdFile = new File("00 Requiem for a dream Instrumental_mp3.fft");
		    Spectrogram s = new Spectrogram(createdFile, 4096);
		    s.show();
		    
			//double[][] results = ReadAudio.fft(f);
		    /*
            double[][] magnitudes = new double[results.length][ReadAudio.CHUNK_SIZE];
			
			double real, complex;
			for (int i = 0; i < results.length; i++) {
			    for (int j = 0; j < ReadAudio.CHUNK_SIZE; j++) {
			        real = results[i][2*j];
			        complex = results[i][2*j+1];
			        magnitudes[i][j] = Math.sqrt(real*real+complex*complex);
			    }
			}
			
			System.out.println("detecting duplicates...");
			boolean same;
			boolean everSame = false;
			for (int i = 0; i < magnitudes.length; i++) {
			    for (int j = i+1; j < magnitudes.length; j++) {
			        same = true;
			        for (int k = 0; k < magnitudes[i].length; k++) {
			            if (magnitudes[i][k] != magnitudes[j][k]) {
			                same = false;
			                break;
			            }
			        }
			        if (same) {
			            everSame = true;
			            System.out.println(i+" and "+j+" are duplicates!!");
			        }
			    }
			}
			
			System.out.println("everSame? "+everSame);
			
			
			System.out.println(magnitudes[0][372]);
			
	        System.out.println(magnitudes[1838][349]);
	        */

			/*
			for (int i = 0; i < 10; i++) {
			    System.out.println(Arrays.toString(magnitudes[i]));
			}
			*/			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}