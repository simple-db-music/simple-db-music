import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class SongLibrary {
	
	private List<String> songs;
	private HashMap<String, SongPoint> database;
	
	private static final String[] names = new String[] { "canon_d_major.wav", "fing_fing_ha.wav", "forrest_gump_theme.wav", 
									  "imagine.wav", "top_of_the_world.wav", "Come Over Full.wav" };
	
	public SongLibrary() {
		songs = new ArrayList<String>();
		database = new HashMap<String, SongPoint>();
	}
	
	public void createDatabase() {
		for (int i = 0; i < names.length; i++) {
			String name = names[i];
			songs.add(name);
			
			double[][] spectrogram = ReadAudio.extractSpectogram("sample_music/" + name, false);	    
		    int[][] keyPoints = ReadAudio.extractKeyPoints(spectrogram);
		    
		    for (int j = 0; j < keyPoints.length; j++) {
		    	SongPoint s = new SongPoint(i, j);
		    	
		    	if (database.containsKey(Arrays.toString(keyPoints[j]))) {
		    		// handle duplicates
		    	} 
		    	
		    	database.put(Arrays.toString(keyPoints[j]), s);
		    }   
		}
	}
	
	// for testing purposes only
	public void matchSong(String file) {
		double[][] spectrogram = ReadAudio.extractSpectogram("sample_music/" + file, false);	    
	    int[][] keyPoints = ReadAudio.extractKeyPoints(spectrogram);
	    
	    matchKeyPoints(keyPoints);
	}
	
	private void matchKeyPoints(int[][] keyPoints) {
		HashMap<Integer, List<Integer>> matches = new HashMap<Integer, List<Integer>>();
		HashMap<Integer, List<Integer>> times = new HashMap<Integer, List<Integer>>();
		
		for (int i = 0; i < keyPoints.length; i++) {
			SongPoint match = database.get(Arrays.toString(keyPoints[i]));
			if (match != null) {
				int songId = match.getId();
				
				if (matches.containsKey(songId) && times.containsKey(songId)) {
					matches.get(songId).add(match.getTime());
					times.get(songId).add(i);
				} else {
					List<Integer> newMatch = new ArrayList<Integer>();
					newMatch.add(match.getTime());
					matches.put(songId, newMatch);
					
					List<Integer> newTime = new ArrayList<Integer>();
					newTime.add(i);
					times.put(songId, newTime);
				}
			}
		}
		
		Integer bestMatch = null;
		double bestPercentage = 0;
		
		for (int songId : matches.keySet()) {
			double percentageMatch = checkTimes(matches.get(songId), times.get(songId));
			
			if (percentageMatch > bestPercentage) {
				bestMatch = songId;
				bestPercentage = percentageMatch;
			}
		}
		
		if (bestMatch != null) {
			System.out.println(songs.get(bestMatch) + " " + bestPercentage);
		} else {
			System.out.println("Unable to find match.");
		}
	}
	
	private double checkTimes(List<Integer> real, List<Integer> recorded) {
		double total = real.size() - 1.0;
		int correct = 0;
		
		for (int i = 1; i < real.size(); i++) {
			int realOffset = real.get(i) - real.get(i - 1);
			int recordedOffset = recorded.get(i) - recorded.get(i - 1);

			if (Math.abs(realOffset - recordedOffset) < 1) correct++;
		}
		
		return correct / total;
	}
}
