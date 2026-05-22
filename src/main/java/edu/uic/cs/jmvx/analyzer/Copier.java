package edu.uic.cs.jmvx.analyzer;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Copier {
    public static void main(String[] args){
        if(args.length != 2){
            System.err.println("Please supply the directory to the recording and a directory to copy to");
        }else{
            Path srcRec = Paths.get(args[0]);
            Path destRec = Paths.get(args[1]);
            RRReader reader = new RRReader(srcRec);
            RRWriter writer = new RRWriter(destRec);
            reader.accept(writer);
        }
    }
}
