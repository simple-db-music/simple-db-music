import java.io.File;

import songs.SongLibrary;

public class Main {
    private static final File KNOWN_SONG_FOLDER = new File("known_songs");
    private static final File SAMPLE_SONG_FOLDER = new File("sample_songs");
    
    
    private static final boolean USE_RANGE_EXTRACTION = true;
    private static final boolean USE_CLUSTERED_DB = false;

    public static void main(String[] args) throws Exception {//IOException, NoSuchElementException, DbException, TransactionAbortedException {
        SongLibrary songLibrary = new SongLibrary(KNOWN_SONG_FOLDER, USE_RANGE_EXTRACTION, USE_CLUSTERED_DB);
        long totalDuration = 0;
        int count = 0;
        for (File sampleSong : SAMPLE_SONG_FOLDER.listFiles()) {
            // Filter out .DS_STORE
            if (sampleSong.getName().startsWith(".")) {
                continue;
            }
            System.out.println("Matching "+sampleSong+"...");
            long duration = songLibrary.matchSong(sampleSong);
            // error reading in sample
            if (duration == -1) {
                continue;
            }
            totalDuration += duration;
            count++;
            System.out.println("Matching took "+ duration + " ms");
        }
        
        System.out.println("\n\nOverall avg matching duration: "+1.0*totalDuration/count+" ms");
    }
}
