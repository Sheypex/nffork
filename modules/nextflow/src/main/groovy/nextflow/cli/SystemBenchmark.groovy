package nextflow.cli

import groovy.util.logging.Slf4j
import nextflow.script.params.InputsList

import java.util.concurrent.Executors
import java.util.function.Consumer

@Slf4j
class SystemBenchmark {

    private boolean local

    SystemBenchmark(boolean local){
        this.local=local
    }

    void renderHardwareDocument(){
        log.info("Render Hardware Document")

        if(local){
            renderForLocalHardware()
        }
        else {
            renderForClusterHardware()
        }
    }

    void renderForLocalHardware(){
        //GFlops

        //Cores


        //cpu events per second
        String cpuEvents = executeSysbenchCommand(["sysbench", "cpu", "run"], "events per second", ":", 1)
        double cpuEventsPerSecond = cpuEvents.toDouble()
        log.info("CPU events per second: " + cpuEventsPerSecond.toString())

        //write speed of memory
        String rawTransferredMiB = executeSysbenchCommand(["sysbench", "memory", "run"], "transferred", "transferred", 1)
        Double transferredMiB = rawTransferredMiB.substring(2, rawTransferredMiB.length()-1).split(" ")[0].toDouble()
        // convert MiB to MB
        Double transferredMB = transferredMiB * 1.048576
        log.info("memory transferred MB per second: " + transferredMB.toString())

    }


    String executeSysbenchCommand(List<String> command, String filterLine, String splitter, int splitIndex){
        //Instantiate the ProcessBuilder and set the command and working directory
        ProcessBuilder processBuilder = new ProcessBuilder(command)
        processBuilder.directory(new File(System.getProperty("user.home")))

        try{
            //execute the command
            Process process = processBuilder.start()
            //Instantiate reader for reading the terminal output
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
            //the output of the command as a String List
            ArrayList<String> output = reader.readLines()
            //get the number of total events from the output lines
            String totalEvents = output.stream()
                    .filter(line -> line.contains(filterLine))
                    .map(line -> line.split(splitter)[splitIndex])
                    .findFirst().toString()
            return removeOptionalFromString(totalEvents)

        } catch (IOException e){
            log.warn("Possible Problem: sysbench not installed")
            e.printStackTrace()
        }
    }

    void renderForClusterHardware(){

    }

    static String removeOptionalFromString(String optional){

        String split1 = optional.split("Optional")[1]
        return split1.substring(1, split1.length()-1)
    }


    static class StreamGobbler implements Runnable{
        private InputStream inputStream
        private Consumer<String> consumer

        StreamGobbler(InputStream inputStream, Consumer<String> consumer){
            this.inputStream = inputStream
            this.consumer = consumer
        }

        @Override
        void run(){
            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer)
        }

    }
}
