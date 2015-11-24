package songs;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
import simpledb.IntField;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;


public class SongLibrary {
	
	private Map<Integer, Set<DataPoint>> database;
	private final Extractor extractor;
	private final String[] songNames;
	private final BTreeFile btree;
	private final TupleDesc td;
	private final TransactionId tid;
	
	public SongLibrary(String[] songNames, boolean useRangeExtraction) throws IOException {
	    this.songNames = songNames;
		database = new HashMap<Integer, Set<DataPoint>>();
		if (useRangeExtraction) {
		    extractor = new RangeExtractor();
		} else {
		    extractor = new AnchorExtractor();
		}
		btree = BTreeUtility.createEmptyBTreeFile("song_db", 3, 0);
        Database.getCatalog().addTable(btree);
		td = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE},
		        new String[] {"Hash", "Time Offset", "Track ID"});
		tid = new TransactionId();
	}
	
	public void createDatabase() {
	    //Set<DataPoint> points;
	    
		for (int i = 0; i < songNames.length; i++) {
			String name = songNames[i];
			Wave wave = new Wave("sample_music/"+name);
			double[][] spectrogram = ReadAudio.extractSpectogram(wave, name, false);	    
		    Set<DataPoint> dataPoints = extractor.extractDataPoints(spectrogram, i);
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
		        //int[] tupData = new int[] {p.getHash(), p.getTimeOffset(), p.getTrackId()};
		        //ArrayBlockingQueue<ArrayList
	            //BTreeInserter inserter = new BTreeInserter(bf, tupData, )
	            
		    }
		    /*
		    for (DataPoint p : dataPoints) {
		        points = database.get(p.getHash());
		        if (points == null) {
		            points = new HashSet<DataPoint>();
		        }
		        points.add(p);
		        database.put(p.getHash(), points);
		    }
		    */
		}
	}
	
	public void matchSong(String file) throws NoSuchElementException, DbException, TransactionAbortedException {
	    Wave wave = new Wave("sample_music/"+file);
		double[][] spectrogram = ReadAudio.extractSpectogram(wave, file, false);
		// assigning a unique song id
		Set<DataPoint> samplePoints = extractor.extractDataPoints(spectrogram, songNames.length);
		Map<Integer, Double> songToScore = extractor.matchPoints(samplePoints, btree, tid);
		
		System.out.println("Scores: "+songToScore.toString());
	}
}
