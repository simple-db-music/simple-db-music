package songs;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.musicg.wave.Wave;

import audio.ReadAudio;
import fingerprint.AnchorExtractor;
import fingerprint.DataPoint;
import fingerprint.Extractor;
import fingerprint.RangeExtractor;


public class SongLibrary {
	
	private Map<Integer, Set<DataPoint>> database;
	private final Extractor extractor;
	private final String[] songNames;
	
	public SongLibrary(String[] songNames, boolean useRangeExtraction) {
	    this.songNames = songNames;
		database = new HashMap<Integer, Set<DataPoint>>();
		if (useRangeExtraction) {
		    extractor = new RangeExtractor();
		} else {
		    extractor = new AnchorExtractor();
		}
	}
	
	public void createDatabase() {
	    Set<DataPoint> points;
		for (int i = 0; i < songNames.length; i++) {
			String name = songNames[i];
			Wave wave = new Wave("sample_music/"+name);
			double[][] spectrogram = ReadAudio.extractSpectogram(wave, name, false);	    
		    Set<DataPoint> dataPoints = extractor.extractDataPoints(spectrogram, i);
		    for (DataPoint p : dataPoints) {
		        points = database.get(p.getHash());
		        if (points == null) {
		            points = new HashSet<DataPoint>();
		        }
		        points.add(p);
		        database.put(p.getHash(), points);
		    }
		}
	}
	
	public void matchSong(String file) {
	    Wave wave = new Wave("sample_music/"+file);
		double[][] spectrogram = ReadAudio.extractSpectogram(wave, file, false);
		// assigning a unique song id
		Set<DataPoint> samplePoints = extractor.extractDataPoints(spectrogram, songNames.length);
		Map<Integer, Double> songToScore = extractor.matchPoints(samplePoints, database);
		
		System.out.println("Scores: "+songToScore.toString());
	}
}
