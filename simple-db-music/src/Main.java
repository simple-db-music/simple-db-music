import javax.sound.sampled.UnsupportedAudioFileException;

public class Main {
    
    private static final String SONG_NAME = "/imagine.wav";
	
	public static void main(String[] args) throws UnsupportedAudioFileException {
		/*try {
		    double[][] spectrogram = ReadAudio.extractSpectogram("sample_music/"+SONG_NAME, false);
		    System.out.println("num lines: "+spectrogram.length);
		    System.out.println("num freqs in line "+spectrogram[0].length);
		    
		    int[][] keypoints = ReadAudio.extractKeyPoints(spectrogram);
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		
		SongLibrary songLibrary = new SongLibrary();
		songLibrary.createDatabase();
		
		long time = System.currentTimeMillis();
		// should find match
		songLibrary.matchSong("Come Over Clip 0.wav");
		songLibrary.matchSong("Come Over Clip 3.wav");
		songLibrary.matchSong("Come Over Clip 4.wav");
		
		// key points not in database, should not find match
		songLibrary.matchSong("Springsteen Clip 0.wav");
		songLibrary.matchSong("Springsteen Clip 3.wav");
		songLibrary.matchSong("hi_bryan.wav");
		
		System.out.println(System.currentTimeMillis() - time + " ms");
	}

}
