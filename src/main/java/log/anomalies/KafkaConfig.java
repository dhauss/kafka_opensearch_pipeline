package log.anomalies;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
/*
 * KafkaConfig has one constructor which takes a filepath that should reference a Kafka config file.
 * Constructor parses the lines in the file and creates 4 private variables: bootstrapServer,
 * saslMechanism, securityProtocol, and saslJaasConfig. These can then be used to create a Kafka
 * client without exposing sensitive data in plain text (e.g. on this public Github repo)
 */
public class KafkaConfig {
    private String bootstrapServer;
    private String saslMechanism;
    private String securityProtocol;
    private String saslJaasConfig;

    public KafkaConfig(String filepath){
        try{
            File config_file = new File(filepath);
            FileReader config_fr = new FileReader(config_file);
            BufferedReader config_br = new BufferedReader((config_fr));
            StringBuffer config_sb = new StringBuffer();
            String line;

            //read in config file
            while((line = config_br.readLine()) != null){
                config_sb.append(line);
                config_sb.append("\n");
            }
            config_fr.close();

            // split file by linebreaks to separate config elements
            String[] configStrings = config_sb.toString().split("\n");

            // config file line 1 specifies bootstrap server, line 2 SASL mechanism, line 3 security protocol
            // formatted as key=value pairs
            this.setBootstrapServer(configStrings[0].split("=")[1]);
            this.setSaslMechanism(configStrings[1].split("=")[1]);
            this.setSecurityProtocol(configStrings[2].split("=")[1]);

            // hacky solution to SASL config containing multiple key=value pairs
            String[] saslConfig = configStrings[3].split("=");
            this.setSaslJaasConfig(saslConfig[1] + "=" + saslConfig[2] + "=" + saslConfig[3]);

        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }
}
