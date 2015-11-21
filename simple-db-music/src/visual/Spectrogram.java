package visual;

import java.lang.System;
import javax.swing.JPanel;
import javax.swing.JComponent;
import javax.swing.JFrame;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class Spectrogram extends JComponent {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final File f;
    private final int lineSize;
    
    public Spectrogram(File f, int lineSize) {
        this.f = f;
        this.lineSize = lineSize;
    }
    
    public void show() {
        JFrame mainFrame = new JFrame("Spectrogram");
        mainFrame.getContentPane().add(this);
        mainFrame.pack();
        mainFrame.setVisible(true);
    }
    
    public Dimension getPreferredSize() {
        return new Dimension(1000, 1000);
    }

    public Dimension getMinimumSize() {
        return getPreferredSize();
    }

    public void paintComponent(Graphics g) {
        super.paintComponent(g);
        super.setBackground(Color.WHITE);
        try {
            paintWithFile(g);
            /*
            g.setColor(Color.BLUE);
            g.fillRect(0, 0, 400, 400);
            */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void paintWithFile(Graphics g) throws IOException {
        DataInputStream inputStream = new DataInputStream(new FileInputStream(f));
        
        int numLines = 10402;
        
        double xSize = 1000.0/numLines;
        double ySize = 1000.0/lineSize;
        
        System.out.println("xSize: "+xSize+", ySize: "+ySize);
        
        double curMag;
        double curReal;
        double curImag;
        double curColor;
        
        g.setColor(Color.BLUE);
        g.fillRect(0, 0, 200, 200);
        
        System.out.println("starting to read...");
        
        for (int i = 0; i < numLines; i++) {
            if (i%100 == 0) {
                System.out.println("cur line: "+i);
                //this.repaint();
            }
            for (int j = 0; j < lineSize; j++) {
                curReal = inputStream.readDouble();
                curImag = inputStream.readDouble();
                curMag = Math.sqrt(curReal*curReal + curImag*curImag);
                
                curColor = Math.log(curMag+1);
                
                int xRect = (int) (xSize*i);
                int yRect = (int) (ySize*j);
                
                //g.setColor(new Color(0,(int)curColor*10,(int)curColor*20));
                g.setColor(Color.RED);
                //g.fillRect(xRect, yRect, 10, 10);
                g.fillRect(0, 0, 300, 300);
                
                //System.out.println("xRect: "+xRect+", yRect: "+yRect);
            }
        }
        System.out.println("done reading!");
        inputStream.close();
    }
}
    