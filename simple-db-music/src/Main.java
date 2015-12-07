import java.io.File;

import audio.JavaSoundRecorder;
import fingerprint.AnchorExtractor;
import fingerprint.Extractor;
import fingerprint.RangeExtractor;
import simpledb.Database;
import songs.SongLibrary;

public class Main {
    
    private static final File KNOWN_SONG_FOLDER = new File("known_songs");
    private static final JavaSoundRecorder recorder = new JavaSoundRecorder();
    //private static final File SAMPLE_SONG_FOLDER = recorder.wavFile.getParentFile();
    private static final File SAMPLE_SONG_FOLDER = new File("sample_songs");
    
    // record duration, in milliseconds
    private static final long RECORD_TIME = 10000;  // 10 seconds 
    
    private static final boolean USE_RANGE_EXTRACTION = true;
    private static final boolean USE_PARALLEL_ANCHOR = false;
    // 2, 3, 5, 10 work best on my (slow) computer
    private static final int NUM_THREADS = 3;

    public static void main(String[] args) throws Exception {//IOException, NoSuchElementException, DbException, TransactionAbortedException {        
        // MRU good for range extraction and parallel anchor extraction
        Database.getBufferPool().setMRU(USE_RANGE_EXTRACTION || !USE_RANGE_EXTRACTION && USE_PARALLEL_ANCHOR);
        
        // creates a new thread that waits for a specified
        // of time before stopping
        Thread stopper = new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(RECORD_TIME);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                recorder.finish();
            }
        });
 
        //stopper.start();
 
        // start recording
        //recorder.start();
        
        Extractor extractor;
        if (USE_RANGE_EXTRACTION) {
            extractor = new RangeExtractor();
        } else {
            extractor = new AnchorExtractor(USE_PARALLEL_ANCHOR, NUM_THREADS);
        }
        
        SongLibrary songLibrary = new SongLibrary(KNOWN_SONG_FOLDER, extractor);
        long totalDuration = 0;
        int count = 0;
        for (File sampleSong : SAMPLE_SONG_FOLDER.listFiles()){
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
