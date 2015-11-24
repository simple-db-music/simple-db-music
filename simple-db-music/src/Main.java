import songs.SongLibrary;

public class Main {
    /*
    private static final String[] KNOWN_SONGS = {"1-07 Can't Feel My Face.wav",
            "Drake - Hotline Bling.wav", "hi_bryan.wav", "09 Jumpman.wav", 
            "canon_d_major.wav", "fing_fing_ha.wav", "forrest_gump_theme.wav", 
            "imagine.wav", "top_of_the_world.wav", "Come Over Full.wav"
    };
    */
    private static final String[] KNOWN_SONGS = {
            "canon_d_major.wav", "fing_fing_ha.wav", "forrest_gump_theme.wav", 
            "imagine.wav", "top_of_the_world.wav", "Come Over Full.wav"
    };
    /*
    private static final String[] KNOWN_SONGS = {"1-07 Can't Feel My Face.wav",
             "hi_bryan.wav", "09 Jumpman.wav", "Drake - Hotline Bling.wav"
    };
    */
    //private static final String SAMPLE_SONG = "hotline bling sample.wav";
    private static final String SAMPLE_SONG = "Come Over Clip 0.wav";
    //private static final String SAMPLE_SONG = "can't feel my face sample.wav";
    
    private static final boolean USE_RANGE_EXTRACTION = true;

    public static void main(String[] args) {
        SongLibrary songLibrary = new SongLibrary(KNOWN_SONGS, USE_RANGE_EXTRACTION);
        songLibrary.createDatabase();
        long time = System.currentTimeMillis();
        songLibrary.matchSong(SAMPLE_SONG);
        System.out.println("Matching took "+(System.currentTimeMillis() - time) + " ms");
    }
}
