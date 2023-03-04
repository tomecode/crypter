package demo;

import java.nio.file.Paths;
import com.encryptor.Cypter;

public class Demo {


  public static final void main(String[] args) throws Exception {

    new Cypter().encrypt(Paths.get("C:\\Workspace\\tome\\encryptor\\src\\test\\resources\\Input.txt"), Paths.get("output.txt"));

  }

}
