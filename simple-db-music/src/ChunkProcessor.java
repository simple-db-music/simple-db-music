import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;

public class ChunkProcessor {

    private static final float SAMPLE_RATE = 44100;
    private static final int NUM_CHANNELS = 2;

    private final AudioInputStream audioInput;
    private int nextByte;
    private final int chunkSize;

    public ChunkProcessor(File audioFile, int chunkSize) throws UnsupportedAudioFileException, IOException {
        this.chunkSize = chunkSize;

        AudioInputStream in= AudioSystem.getAudioInputStream(audioFile);
        AudioFormat baseFormat = in.getFormat();
        if (baseFormat.getSampleRate() != SAMPLE_RATE) {
            System.out.println("Warning: Converting sample rates");
        }
        AudioFormat decodedFormat = new AudioFormat(AudioFormat.Encoding.PCM_SIGNED, 
                SAMPLE_RATE,
                16,
                NUM_CHANNELS,
                NUM_CHANNELS * 2,
                SAMPLE_RATE,
                false);
        this.audioInput = AudioSystem.getAudioInputStream(decodedFormat, in);
        AudioFormat f = audioInput.getFormat();
        System.out.println("sample rate: "+f.getSampleRate());
        System.out.println("sample bits: "+f.getSampleSizeInBits());
        System.out.println("frame rate: "+f.getFrameRate());

        System.out.println("num chunks: "+f.getSampleRate()*(f.getSampleSizeInBits()/8)*audioInput.getFrameLength()/f.getFrameRate());
        nextByte = this.audioInput.read();    
        //int i = AudioSystem.NOT_SPECIFIED;
        System.out.println("frame length: "+audioInput.getFrameLength());

    }    
    
    public boolean hasChunksLeft() {
        return nextByte != -1;
    }
    
    public List<Byte> getNextChunk() throws IOException {
        if (nextByte == -1) {
            return null;
        }
        
        List<Byte> bytes = new ArrayList<Byte>();
        
        for (int i = 0; i < chunkSize; i++) {
            bytes.add( (byte) nextByte);
            nextByte = audioInput.read();
            if (nextByte == -1) {
                break;
            }
        }
        
        return bytes;
    }

}
