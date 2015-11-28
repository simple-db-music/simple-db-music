package songs;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import simpledb.IntField;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;


public class SongLibrary {
	
    
    
    private final File songNameFile = new File("song_names.txt");
    private final File dbFile = new File("song_db");
    
	private final Extractor extractor;
	private final List<String> dbSongs;
	private final BTreeFile btree;
	private final TupleDesc td;
	private final TransactionId tid;
	
	public SongLibrary(String[] songNames, boolean useRangeExtraction) throws IOException {
		if (useRangeExtraction) {
		    extractor = new RangeExtractor();
		} else {
		    extractor = new AnchorExtractor();
		}
		if (!dbFile.exists()) {
		    dbFile.createNewFile();
		}
		btree = BTreeUtility.openBTreeFile(3, dbFile, 0);
        Database.getCatalog().addTable(btree);
		td = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE},
		        new String[] {"Hash", "Time Offset", "Track ID"});
		tid = new TransactionId();
		dbSongs = readInSongNames();
		System.out.println("dbSongs after reading in: "+dbSongs);
		addMissingSongs(songNames);
	}
	
	private List<String> readInSongNames() throws IOException {
	    if (!songNameFile.exists()) {
	        songNameFile.createNewFile();
	    }
	    FileInputStream inputStream = new FileInputStream(songNameFile);
	    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
	    List<String> songNames = new ArrayList<String>();
	    String curLine = in.readLine();
	    while(curLine != null) {
	        songNames.add(curLine);
	        curLine = in.readLine();
	    }
	    in.close();
	    return songNames;
	}
	
	private void addMissingSongs(String[] songNames) throws IOException {
	    BufferedWriter bw = new BufferedWriter(new FileWriter(songNameFile, true));
	    for (int i = 0; i < songNames.length; i++) {
	        String name = songNames[i];
	        if (!dbSongs.contains(name)) {
	            Wave wave = new Wave("sample_music/"+name);
	            double[][] spectrogram = ReadAudio.extractSpectogram(wave, name, false);        
	            Set<DataPoint> dataPoints = extractor.extractDataPoints(spectrogram, dbSongs.size());
	            System.out.println("assigned song id "+dbSongs.size()+" to song "+name);
	            for (DataPoint p  : dataPoints) {
	                Tuple tupleDataPoint = new Tuple(td);
	                tupleDataPoint.setField(0, new IntField(p.getHash()));
	                tupleDataPoint.setField(1, new IntField(p.getTimeOffset()));
	                tupleDataPoint.setField(2, new IntField(p.getTrackId()));
	                try {
	                    Database.getBufferPool().insertTuple(tid, btree.getId(), tupleDataPoint);
	                } catch (Exception e) {
	                    e.printStackTrace();
	                }
	            }
	            dbSongs.add(name);
	            bw.write(name);
	            bw.newLine();
	        }
	    }
	    bw.close();
	}
	
	public long matchSong(String file) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
	    Wave wave = new Wave("sample_music/"+file);
		double[][] spectrogram = ReadAudio.extractSpectogram(wave, file, false);
		// assigning a unique song i
		Set<DataPoint> samplePoints = extractor.extractDataPoints(spectrogram, -1);
        long time = System.currentTimeMillis();
		Map<Integer, Double> songIdToScore = extractor.matchPoints(samplePoints, btree, tid);
		Map<String, Double> songToScore = new HashMap<String, Double>();
		for (Entry<Integer, Double> track : songIdToScore.entrySet()) {
		    songToScore.put(dbSongs.get(track.getKey()), track.getValue());
		}
		
		System.out.println("Scores: "+songToScore.toString());
		Database.getBufferPool().flushAllPages();
		return System.currentTimeMillis() - time;
	}
}
