package nextflow.cli

import groovy.util.logging.Slf4j
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path


@Slf4j
/**
 * Renders a simgrid XML file describing the local executing hardware.
 * Can be used for simulations in WRENCH (https://wrench-project.org/)
 * @author Frederic Risling
 */
class LocalSystemBenchmark  implements SystemBenchmark{

    /**
     * states whether a benchmark should be executed or not
     * add - benchmark = 'true' - to the nextflow.config in the working directory to execute a benchmark
     */
    private boolean benchmark

    /**
     * constructor for a LocalSystemBenchmark instance
     */
    LocalSystemBenchmark(){
        this.benchmark = checkIfBenchmarkConfigured()
    }

    /**
     * implementation of renderHardwareDocument()
     * should be used for local execution or single cloud instance execution
     */
    void renderHardwareDocument(){
        if (!benchmark) {
            log.info("Benchmark turned off -> add benchmark = 'true' to nextflow.config if required")
            return
        }

        log.info("Render Local Hardware Document")

        //Benchmark GFlops
        log.info("Benchmarking FLOPS ...")
        String gFlops = executeGFlopsBenchmark()
        log.info("$gFlops GFlops")

        //Get number of cores
        ArrayList<String> coresRaw = executeCommand(["getconf","_NPROCESSORS_ONLN"])
        String cores = coresRaw.first()

        //read speed fileio
        String rawReadSpeedData = executeSysbenchCommand(["/bin/bash", "-c", "sysbench --file-test-mode=seqrd fileio prepare;sysbench --file-test-mode=seqrd fileio run"], \
             "read, MiB/s", ":", 1)
        Double readSpeedDataMiB = rawReadSpeedData.toDouble()
        //convert to MBps
        String readSpeed = (readSpeedDataMiB * 1.048576).round(3).toString()

        //write speed fileio
        String rawWriteSpeedData = executeSysbenchCommand(["/bin/bash", "-c", "sysbench --file-test-mode=seqwr fileio prepare;sysbench --file-test-mode=seqwr fileio run"], \
             "written, MiB/s", ":", 1)
        Double writeSpeedDataMiB = rawWriteSpeedData.toDouble()
        //convert to MBps
        String writeSpeed = (writeSpeedDataMiB * 1.048576).round(3).toString()

        //disk size
        File file = new File("/")
        //Convert Byte to Gibibyte (GiB) by dividing by 1.074e+9
        int space = file.totalSpace/1.074e+9

        //write the local_host.xml file with the benchmark results from above
        log.info("Writing the host file to: ")
        writeHostsXMLFileLocal(gFlops, cores, readSpeed, writeSpeed, space.toString())

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
            log.info("FEEEEEHLER")
            e.printStackTrace()
        }
    }


    private static String executeGFlopsBenchmark(){
        //docker run -it --rm h20180061/linpack
        List<String> output = executeCommand(["/bin/bash", "-c" ,"docker run -e LINPACK_ARRAY_SIZE=150 h20180061/linpack"])

        int count = output.stream()
                .filter(it -> it.contains("%"))
                .count()

        Double sum = output.stream()
                .filter(it -> it.contains("%"))
                .map(it->Double.parseDouble(it.split(" ").last()))
                .reduce(0.0, (x,y)-> x+y)

        Double avg = sum/count
        (avg/10000).round(3).toString()

    }


    private static String executeSysbenchCommand(List<String> command, String filterLine, String splitter, int splitIndex){
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


    private static void writeHostsXMLFileLocal(String gFlops, String cores, String readSpeed, String writeSpeed, String diskSize){
        Path path = new File((System.getProperty("user.dir")+"/local_host.xml")).toPath()
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
        writeHostsLocal(w, gFlops, cores, readSpeed, writeSpeed, diskSize)

        //write links
        writeLinksLocal(w)

        //write network routes
        writeRoutesLocal(w)

        //write loopback routes
        writeLoopbackRoutesLocal(w)

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


    private static void writeHostsLocal(XMLStreamWriter w, String gFlops, String cores, String readSpeed, String writeSpeed, String diskSize){
        //<host id="Host1" speed="xyzGf" core="xy"/>
        w.writeCharacters("\t\t")
        w.writeStartElement("host")
        w.writeAttribute("id", "Host1")
        w.writeAttribute("speed", gFlops + "Gf")
        w.writeAttribute("core", cores)
        w.writeEndElement()
        w.writeCharacters("\n")

        //<host id="Host2" speed="xyzGf" core="xy"/>
        w.writeCharacters("\t\t")
        w.writeStartElement("host")
        w.writeAttribute("id", "Host2")
        w.writeAttribute("speed", gFlops + "Gf")
        w.writeAttribute("core", cores)
        w.writeEndElement()
        w.writeCharacters("\n")

        //<host id="Host3" speed="xyzGf" core="xy">
        w.writeCharacters("\t\t")
        w.writeStartElement("host")
        w.writeAttribute("id", "Host3")
        w.writeAttribute("speed", gFlops + "Gf")
        w.writeAttribute("core", cores)
        //<disk id="large_disk" read_bw="xyzMBps" write_bw="xyzMBps">
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("disk")
        w.writeAttribute("id", "large_disk")

        //WRENCH requires: readSpeed==writeSpeed --> Thus, we use the average of both
        String avgDiskSpeed = ((readSpeed.toDouble()+writeSpeed.toDouble())/2).round(3).toString()
        w.writeAttribute("read_bw", avgDiskSpeed+"MBps")
        w.writeAttribute("write_bw", avgDiskSpeed+"MBps")

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


    private static void writeLinksLocal(XMLStreamWriter w){
        //<!-- A network link -->
        w.writeCharacters("\n\n\t\t")
        w.writeComment(" A network link ")
        //<link id="network_link" bandwidth="5000GBps" latency="0us"/>
        w.writeCharacters("\n\t\t")
        w.writeStartElement("link")
        w.writeAttribute("id", "network_link")
        w.writeAttribute("bandwidth", "5000GBps")
        w.writeAttribute("latency", "0us")
        w.writeEndElement()

        //<!-- Host1's local "loopback" link...-->
        w.writeCharacters("\n\t\t")
        w.writeComment(" Host1's local \"loopback\" link...")
        //<link id="loopback_Host1" bandwidth="1000EBps" latency="0us"/>
        w.writeCharacters("\n\t\t")
        w.writeStartElement("link")
        w.writeAttribute("id", "loopback_Host1")
        w.writeAttribute("bandwidth", "1000EBps")
        w.writeAttribute("latency", "0us")
        w.writeEndElement()

        //<!-- Host2's local "loopback" link...-->
        w.writeCharacters("\n\t\t")
        w.writeComment(" Host2's local \"loopback\" link...")
        //<link id="loopback_Host2" bandwidth="1000EBps" latency="0us"/>
        w.writeCharacters("\n\t\t")
        w.writeStartElement("link")
        w.writeAttribute("id", "loopback_Host2")
        w.writeAttribute("bandwidth", "1000EBps")
        w.writeAttribute("latency", "0us")
        w.writeEndElement()

        //<!-- Host3's local "loopback" link...-->
        w.writeCharacters("\n\t\t")
        w.writeComment(" Host3's local \"loopback\" link...")
        //<link id="loopback_Host3" bandwidth="1000EBps" latency="0us"/>
        w.writeCharacters("\n\t\t")
        w.writeStartElement("link")
        w.writeAttribute("id", "loopback_Host3")
        w.writeAttribute("bandwidth", "1000EBps")
        w.writeAttribute("latency", "0us")
        w.writeEndElement()
    }


    private static void writeRoutesLocal(XMLStreamWriter w){
        w.writeCharacters("\n\n\n\t\t")
        //<!-- The network link connects all hosts together -->
        w.writeComment(" The network link connects all hosts together ")

        //<route src="Host1" dst="Host2">
        w.writeCharacters("\n\t\t")
        w.writeStartElement("route")
        w.writeAttribute("src", "Host1")
        w.writeAttribute("dst", "Host2")
        //  <link_ctn id="network_link"/>
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("link_ctn")
        w.writeAttribute("id", "network_link")
        w.writeEndElement()
        //</route>
        w.writeCharacters("\n\t\t")
        w.writeEndElement()

        //<route src="Host1" dst="Host3">
        w.writeCharacters("\n\t\t")
        w.writeStartElement("route")
        w.writeAttribute("src", "Host1")
        w.writeAttribute("dst", "Host3")
        //  <link_ctn id="network_link"/>
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("link_ctn")
        w.writeAttribute("id", "network_link")
        w.writeEndElement()
        //</route>
        w.writeCharacters("\n\t\t")
        w.writeEndElement()

        //<route src="Host2" dst="Host3">
        w.writeCharacters("\n\t\t")
        w.writeStartElement("route")
        w.writeAttribute("src", "Host2")
        w.writeAttribute("dst", "Host3")
        //  <link_ctn id="network_link"/>
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("link_ctn")
        w.writeAttribute("id", "network_link")
        w.writeEndElement()
        //</route>
        w.writeCharacters("\n\t\t")
        w.writeEndElement()

    }


    private static void writeLoopbackRoutesLocal(XMLStreamWriter w){
        w.writeCharacters("\n\n\n\t\t")
        //<!-- Each loopback link connects each host to itself -->
        w.writeComment(" Each loopback link connects each host to itself ")

        //<route src="Host1" dst="Host1">
        w.writeCharacters("\n\t\t")
        w.writeStartElement("route")
        w.writeAttribute("src", "Host1")
        w.writeAttribute("dst", "Host1")
        //  <link_ctn id="network_link"/>
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("link_ctn")
        w.writeAttribute("id", "loopback_Host1")
        w.writeEndElement()
        //</route>
        w.writeCharacters("\n\t\t")
        w.writeEndElement()

        //<route src="Host2" dst="Host2">
        w.writeCharacters("\n\t\t")
        w.writeStartElement("route")
        w.writeAttribute("src", "Host2")
        w.writeAttribute("dst", "Host2")
        //  <link_ctn id="network_link"/>
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("link_ctn")
        w.writeAttribute("id", "loopback_Host2")
        w.writeEndElement()
        //</route>
        w.writeCharacters("\n\t\t")
        w.writeEndElement()

        //<route src="Host3" dst="Host3">
        w.writeCharacters("\n\t\t")
        w.writeStartElement("route")
        w.writeAttribute("src", "Host3")
        w.writeAttribute("dst", "Host3")
        //  <link_ctn id="network_link"/>
        w.writeCharacters("\n\t\t\t")
        w.writeStartElement("link_ctn")
        w.writeAttribute("id", "loopback_Host3")
        w.writeEndElement()

        //</route>
        w.writeCharacters("\n\t\t")
        w.writeEndElement()

    }


    static boolean checkIfBenchmarkConfigured(){
        File file = new File(System.getProperty("user.dir")+"/nextflow.config")
        Scanner scanner = new Scanner(file)
        int lineNum = 0
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            lineNum++
            if(line.contains("benchmark")) {
                return true
            }
        }
        false
    }


    private static String removeOptionalFromString(String optional){
        String split1 = optional.split("Optional")[1]
        return split1.substring(1, split1.length()-1)
    }

}




