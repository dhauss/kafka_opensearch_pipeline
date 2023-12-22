package log.anomalies;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
/*
 * BonsaiConfig has one constructor which takes a filepath that should reference a Bonsai config file.
 * Constructor reads in the single line of the file and initializes connString attribute. This can then
 * be used to create a Bonsai client without exposing sensitive data in plain text
 * (e.g. on this public Github repo)
 */
public class BonsaiConfig {
    private String connString;

    public BonsaiConfig(String filepath){
        try {
            File config_file = new File(filepath);
            FileReader config_fr = new FileReader(config_file);
            BufferedReader config_br = new BufferedReader((config_fr));
            StringBuffer config_sb = new StringBuffer();

            // file consists of single line- connection URI
            this.setConnString(config_br.readLine().toString());

            config_fr.close();
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public String getConnString() { return connString; }

    public void setConnString(String connString) { this.connString = connString; }
}
