package songs;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.musicg.wave.Wave;

import audio.ReadAudio;
import fingerprint.AnchorExtractor;
import fingerprint.DataPoint;
import fingerprint.Extractor;
import fingerprint.RangeExtractor;
import simpledb.BTreeFile;
import simpledb.BTreeUtility;
import simpledb.Database;
import simpledb.DbException;
import simpledb.DbFileIterator;
import simpledb.HeapFile;
import simpledb.IntField;
import simpledb.SeqScan;
import simpledb.StringField;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.Utility;


public class SongLibrary {
    private final File dbFile;
    private final File songNameFile = new File("songs");

    private final Extractor extractor;
    private final BTreeFile btree;
    private final HeapFile songNameTable;
    private final TupleDesc btreeTd;
    private final TupleDesc songNameTableTd;
    private final TransactionId tid;

    public SongLibrary(File songFolder, boolean useRangeExtraction, boolean useClustered) throws IOException {
        if (useRangeExtraction) {
            extractor = new RangeExtractor();
        } else {
            extractor = new AnchorExtractor();
        }
        if (useClustered) {
            dbFile = new File("clustered_song_db");
        } else {
            dbFile = new File("song_db");
        }
        
        btreeTd = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE},
                new String[] {"Hash", "Time Offset", "Track ID"});
        songNameTableTd = new TupleDesc(new Type[]{Type.STRING_TYPE, Type.INT_TYPE},
                new String[]{"Song Name", "Track ID"});
        tid = new TransactionId();
        boolean needToInitDb = !dbFile.exists();
        if (needToInitDb) {
            btree = BTreeUtility.createEmptyBTreeFile("song_db", 3, 0);
            songNameTable = Utility.createEmptyHeapFile("songs", 2, songNameTableTd);
            Database.getCatalog().addTable(btree);
            Database.getCatalog().addTable(songNameTable);
            createDatabase(songFolder);
        } else {
            btree = BTreeUtility.openBTreeFile(3, dbFile, 0);
            Database.getCatalog().addTable(btree);
            songNameTable = Utility.openHeapFile(2, songNameFile, songNameTableTd);
            Database.getCatalog().addTable(songNameTable);
        }
        //createDatabase(songFolder);
    }

    private void createDatabase(File songFolder) {
        System.out.println("creating db...");
        int songNum = 0;
        int tupCount = 0;
        for (File song : songFolder.listFiles()) {
            String name = song.getName();
            // filter out .DS_STORE
            if (name.startsWith(".")) {
                continue;
            }
            System.out.println("on song "+songNum);
            Wave wave = new Wave(song.getAbsolutePath());
            double[][] spectrogram = ReadAudio.extractSpectogram(wave, name, false);
            // error reading in song
            if (spectrogram == null) {
                continue;
            }
            Set<DataPoint> dataPoints = extractor.extractDataPoints(spectrogram, songNum);
            System.out.println(name+" has "+dataPoints.size());
            tupCount += dataPoints.size();
            for (DataPoint p  : dataPoints) {
                Tuple tupleDataPoint = new Tuple(btreeTd);
                tupleDataPoint.setField(0, new IntField(p.getHash()));
                tupleDataPoint.setField(1, new IntField(p.getTimeOffset()));
                tupleDataPoint.setField(2, new IntField(p.getTrackId()));
                try {
                    Database.getBufferPool().insertTuple(tid, btree.getId(), tupleDataPoint);
                    tupCount++;
                } catch (Exception e) {
                    System.out.println("happening on song "+songNum);
                    e.printStackTrace();
                    try {
                        Database.getBufferPool().flushAllPages();
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                    System.out.println("error creating db, exiting early. "+tupCount+" tuples inserted");
                    System.exit(0);
                }
            }
            Tuple songTuple = new Tuple(songNameTableTd);
            songTuple.setField(0, new StringField(name, Type.STRING_LEN));
            songTuple.setField(1, new IntField(songNum));
            try {
                Database.getBufferPool().insertTuple(tid, songNameTable.getId(), songTuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("done!");
            songNum++;
        }
        System.out.println("total points extracted: "+tupCount);
        try {
            System.out.println("Flushing pages...");
            Database.getBufferPool().flushAllPages();
            System.out.println("done!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            DbFileIterator it = btree.iterator(tid);
            it.open();
            int readCount = 0;
            while (it.hasNext()) {
                readCount++;
                if (readCount % 100000 == 0) System.out.println("read "+readCount+"tuples");
                it.next();
            }
            System.out.println("Read "+readCount+"while scanning over db");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Uncomment this to create a clustered version of the database in a separate file
        // clusterDb();
    }
    
    private void clusterDb() {
        try {
            System.out.println("clustering db...");
            BTreeFile clusteredDb = BTreeUtility.createEmptyBTreeFile("clustered_song_db", 3, 0);
            Database.getCatalog().addTable(clusteredDb);
            DbFileIterator it = btree.iterator(tid);
            it.open();
            int count = 0;
            while (it.hasNext()) {
                count++;
                Database.getBufferPool().insertTuple(tid, clusteredDb.getId(), it.next());
                if (count % 100000 == 0) {
                    System.out.println(count+" tuples inserted");
                }
            }
            it.close();
            System.out.println("Succesfully clustered db! "+count+" tuples inserted");
            //Database.getBufferPool().flushAllPages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long matchSong(File file) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        long time = System.currentTimeMillis();
        Wave wave = new Wave(file.getAbsolutePath());
        double[][] spectrogram = ReadAudio.extractSpectogram(wave, file.getName(), false);
        if (spectrogram == null) {
            // error reading in sample
            return -1;
        }
        // assigning a unique song id
        Set<DataPoint> samplePoints = extractor.extractDataPoints(spectrogram, -1);
        try {
            Map<Integer, Double> songIdToScore = extractor.matchPoints(samplePoints, btree, tid);
            long duration = System.currentTimeMillis() - time;
            Map<String, Double> convertedScores = convertToSongNames(songIdToScore);
            System.out.println("Scores: ");
            convertedScores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(3)
                .forEach(System.out::println);
            System.out.println("end scores");
            //System.out.println("Scores: "+convertedScores.);
            Database.getBufferPool().flushAllPages();
            return duration;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }
    
    private Map<String, Double> convertToSongNames(Map<Integer, Double> songIdToScore) throws TransactionAbortedException, DbException {
        int numSongs = songIdToScore.size();
        Map<String, Double> songToScore = new HashMap<String, Double>();
        SeqScan f = new SeqScan(tid, songNameTable.getId());
        f.open();
        while (f.hasNext()) {
            if (songToScore.size() == numSongs) {
                break;
            }
            Tuple curTup = f.next();
            int trackId = ((IntField) curTup.getField(1)).getValue();
            if (songIdToScore.containsKey(trackId)) {
                String songName = ((StringField) curTup.getField(0)).getValue();
                songToScore.put(songName, songIdToScore.get(trackId));
            }
        }
        return songToScore;
    }
    
    public Map<String, Double> getDatabaseFrequencies() throws NoSuchElementException, DbException, TransactionAbortedException {
        
        Map<Integer, Double> songToCount = new HashMap<Integer, Double>();
        DbFileIterator it = btree.iterator(tid);
        it.open();
        int count = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            int trackId = ((IntField) t.getField(2)).getValue();
            Double d = songToCount.get(trackId);
            if (d == null) {
                d = (double) 0;
            }
            d++;
            count++;
            songToCount.put(trackId, d);
            if (count % 100000 == 0) {
                System.out.println(count +" tuples processed");
            }
         }
        System.out.println(count+ " tuples in db");
        return convertToSongNames(songToCount);
            
    }
}
