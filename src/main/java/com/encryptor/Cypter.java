package com.encryptor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

public final class Cypter {

  // private static final int MAPPED_TRANSFER_SIZE = 2;

  public void encrypt(Path source, Path destination) throws Exception {
    if (!Files.exists(source, LinkOption.NOFOLLOW_LINKS)) {
      throw new FileNotFoundException("Not found input file:" + source);
    }

    CypterProcessor<String> processor = new CypterProcessor<String>();

    new Thread(new Runnable() {

      @Override
      public void run() {

        while (true) {
          try {
            System.out.println("take for encryption:  " + processor.take());
          } catch (InterruptedException e) {
          }
        }
      }
    }).start();
    
    String chunk = null;
    try (BufferedReader br = Files.newBufferedReader(source)) {
      while ((chunk = br.readLine()) != null) {
        processor.addElement(chunk);
      }
    }



  }


}
