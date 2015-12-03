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
    private final File dbFile = new File("song_db");
    private final File songNameFile = new File("songs");

    private final Extractor extractor;
    private final BTreeFile btree;
    private final HeapFile songNameTable;
    private final TupleDesc btreeTd;
    private final TupleDesc songNameTableTd;
    private final TransactionId tid;

    public SongLibrary(String[] songNames, boolean useRangeExtraction) throws IOException {
        if (useRangeExtraction) {
            extractor = new RangeExtractor();
        } else {
            extractor = new AnchorExtractor();
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
            createDatabase(songNames);
        } else {
            btree = BTreeUtility.openBTreeFile(3, dbFile, 0);
            Database.getCatalog().addTable(btree);
            songNameTable = Utility.openHeapFile(2, songNameFile, songNameTableTd);
            Database.getCatalog().addTable(songNameTable);
        }
    }

    private void createDatabase(String[] songNames) {
        System.out.println("creating db...");
        for (int i = 0; i < songNames.length; i++) {
            String name = songNames[i];
            Wave wave = new Wave("sample_music/"+name);
            double[][] spectrogram = ReadAudio.extractSpectogram(wave, name, false);	    
            Set<DataPoint> dataPoints = extractor.extractDataPoints(spectrogram, i);
            for (DataPoint p  : dataPoints) {
                Tuple tupleDataPoint = new Tuple(btreeTd);
                tupleDataPoint.setField(0, new IntField(p.getHash()));
                tupleDataPoint.setField(1, new IntField(p.getTimeOffset()));
                tupleDataPoint.setField(2, new IntField(p.getTrackId()));
                try {
                    Database.getBufferPool().insertTuple(tid, btree.getId(), tupleDataPoint);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Tuple songTuple = new Tuple(songNameTableTd);
            songTuple.setField(0, new StringField(name, Type.STRING_LEN));
            songTuple.setField(1, new IntField(i));
            try {
                Database.getBufferPool().insertTuple(tid, songNameTable.getId(), songTuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("done!");
        }
    }

    public long matchSong(String file) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        Wave wave = new Wave(file);
        double[][] spectrogram = ReadAudio.extractSpectogram(wave, file, false);
        // assigning a unique song id
        Set<DataPoint> samplePoints = extractor.extractDataPoints(spectrogram, -1);
        long time = System.currentTimeMillis();
        Map<Integer, Double> songIdToScore = extractor.matchPoints(samplePoints, btree, tid);
        long duration = System.currentTimeMillis() - time;
        System.out.println("Scores: "+convertToSongNames(songIdToScore).toString());
        Database.getBufferPool().flushAllPages();
        return duration;
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
}
