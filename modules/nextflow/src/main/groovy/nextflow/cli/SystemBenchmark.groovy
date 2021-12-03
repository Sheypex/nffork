package nextflow.cli

import groovy.util.logging.Slf4j
import nextflow.script.params.InputsList
import org.w3c.dom.Document

import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter
import javax.xml.transform.OutputKeys
import javax.xml.transform.Transformer
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.function.Consumer

@Slf4j
class SystemBenchmark {

    private final String SIMGRID_DTD = "\"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\""
    private final String VERSION = "4.1"
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
        log.info("Benchmarking FLOPS ...")
        String gflops = executeArrayFireScript()
        log.info("$gflops GFlops")

        //Cores
        ArrayList<String> coresRaw = executeCommand(["getconf","_NPROCESSORS_ONLN"])
        String cores = coresRaw.first()

        //write speed of memory
        String rawTransferredMiB = executeSysbenchCommand(["sysbench", "memory", "run"], "transferred", "transferred", 1)
        Double transferredMiB = rawTransferredMiB.substring(2, rawTransferredMiB.length()-1).split(" ")[0].toDouble()
        // convert MiB to MB
        Double transferredMB = transferredMiB * 1.048576
        log.info("memory transferred MB per second: " + transferredMB.toString())

        //disk size
        File file = new File("/")
        //Convert Byte to Gibibyte (GiB) by dividing by 1.074e+9
        int space = file.totalSpace/1.074e+9
        log.info("total space: " + space.toString())

        writeHostsXMLFileLocal(gflops, cores, transferredMB.toString(), space.toString())
    }

    String executeArrayFireScript(){
        ProcessBuilder processBuilder = new ProcessBuilder()
        processBuilder.directory(new File(System.getProperty("user.home")))
        //the command for executing the blas_cpu test from ArrayFire
        Process process = processBuilder.command(
            ["bash", "-c", "cp -r /opt/arrayfire/share/ArrayFire/examples /tmp/examples ; cd /tmp/examples ; mkdir build ; cd build ; cmake -DASSETS_DIR:PATH=/tmp .. ; make ; cd benchmarks ; ./blas_cpu"]
        )
                .start()

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
        ArrayList<String> lines = reader.readLines()

        //filter the line which contains the peak GFLOPS
        String result = removeOptionalFromString(lines.stream() \
                                          .filter(line -> line.contains("peak")).findFirst().toString())

        //parse the GFLOPS value from the string
        String result_cleaned = result.split("peak ")[1].split(" ")[0]

        return result_cleaned
    }

    static List<String> executeCommand(List<String> command){
        ProcessBuilder processBuilder = new ProcessBuilder(command)
        processBuilder.directory(new File(System.getProperty("user.home")))
        try{
            //execute the command
            Process process = processBuilder.start()
            //Instantiate reader for reading the terminal output
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
            //the output of the command as a String List
            ArrayList<String> output = reader.readLines()
            //return the result
            return output
        }catch(IOException e){
            e.printStackTrace()
        }
    }

    static String executeSysbenchCommand(List<String> command, String filterLine, String splitter, int splitIndex){
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

    void writeHostsXMLFileLocal(String gflops, String cores, String transferSpeed, String diskSize){
        Path path = new File((System.getProperty("user.dir")+"/batch_host_local.xml")).toPath()
        final Charset charset = Charset.defaultCharset()
        Writer bw = Files.newBufferedWriter(path, charset)
        final XMLOutputFactory xof = XMLOutputFactory.newFactory()
        final XMLStreamWriter w = xof.createXMLStreamWriter(bw)

        //<?xml version='1.0'?>
        w.writeStartDocument(charset.displayName(), "1.0")
        w.writeCharacters("\n")

        //<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd">
        w.writeDTD("<!DOCTYPE platform SYSTEM $SIMGRID_DTD>")
        w.writeCharacters("\n")

        //<platform version="4.1">
        w.writeStartElement("platform")
        w.writeAttribute("version", VERSION)
        w.writeCharacters("\n")

        //<zone id="AS0" routing="Full">
        w.writeCharacters("\t")
        w.writeStartElement("zone")
        w.writeAttribute("id", "AS0")
        w.writeAttribute("routing", "Full")
        w.writeCharacters("\n")

        //write hosts
        writeHostsLocal(w, gflops, cores, transferSpeed, diskSize)

        //write links
        writeLinksLocal(w)

        //write routes
        writeRoutesLocal(w)

        //</zone>
        w.writeCharacters("\n\t")
        w.writeEndElement()


        //</platform>
        w.writeCharacters("\n")
        w.writeEndElement()
        w.writeEndDocument()
        w.flush()
        bw.flush()
        bw.close()

    }

    void writeHostsLocal(XMLStreamWriter w, String gflops, String cores, String transferSpeed, String diskSize){
        //<host id="Host1" speed="xyzGf" core="xy"/>
        w.writeCharacters("\t\t")
        w.writeStartElement("host")
        w.writeAttribute("id", "Host1")
        w.writeAttribute("speed", gflops + "Gf")
        w.writeAttribute("core", cores)
        w.writeEndElement()
        w.writeCharacters("\n")

        //<host id="Host2" speed="xyzGf" core="xy"/>
        w.writeCharacters("\t\t")
        w.writeStartElement("host")
        w.writeAttribute("id", "Host2")
        w.writeAttribute("speed", gflops + "Gf")
        w.writeAttribute("core", cores)
        w.writeEndElement()
        w.writeCharacters("\n")

        //<host id="Host3" speed="xyzGf" core="xy">
        w.writeCharacters("\t\t")
        w.writeStartElement("host")
        w.writeAttribute("id", "Host3")
        w.writeAttribute("speed", gflops + "Gf")
        w.writeAttribute("core", cores)
        //<disk id="large_disk" read_bw="xyzMBps" write_bw="xyzMBps">
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("disk")
        w.writeAttribute("id", "large_disk")
        String transferSpeedRounded = transferSpeed.toDouble().round(0).toInteger().toString()
        w.writeAttribute("read_bw", transferSpeedRounded+"MBps")
        w.writeAttribute("write_bw", transferSpeedRounded+"MBps")
        //<prop id="size" value="XYZGiB"/>
        w.writeCharacters("\n\t\t\t\t")
        w.writeStartElement("prop")
        w.writeAttribute("id", "size")
        w.writeAttribute("value", diskSize+"GiB")
        w.writeEndElement()
        //<prop id="mount" value="/"/>
        w.writeCharacters("\n\t\t\t\t")
        w.writeStartElement("prop")
        w.writeAttribute("id", "mount")
        w.writeAttribute("value","/")
        w.writeEndElement()

        //</disk>
        w.writeCharacters("\n\t\t\t")
        w.writeEndElement()

        //</host>
        w.writeCharacters("\n\t\t")
        w.writeEndElement()

    }

    void writeLinksLocal(XMLStreamWriter){

    }

    void writeRoutesLocal(XMLStreamWriter){

    }

    void renderForClusterHardware(){

    }

    static String removeOptionalFromString(String optional){

        String split1 = optional.split("Optional")[1]
        return split1.substring(1, split1.length()-1)
    }

}


