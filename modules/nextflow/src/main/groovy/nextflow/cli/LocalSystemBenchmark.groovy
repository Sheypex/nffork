package nextflow.cli

import groovy.util.logging.Slf4j
import nextflow.container.DockerBuilder
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

    private boolean local

    /**
     * format of the file (!DOCTYPE)
     */
    private final String SIMGRID_DTD = "\"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\""

    /**
     * Simgrid version of the file
     */
    private final String VERSION = "4.1"

    /**
     * constructor for a LocalSystemBenchmark instance
     */
    LocalSystemBenchmark(){
        if(new File((System.getProperty("user.dir")+"/nextflow.config")).isFile()){
            this.local = !checkIfSlurmConfigured()
        }
        else{
            this.local = true
        }
    }

    /**
     * implementation of renderHardwareDocument()
     * should be used for local execution or single cloud instance execution
     */
    void renderHardwareDocument(){
        if(local){
            log.info("Render Local Hardware Document")
            renderForLocalHardware()
        }
        else {
            log.info("Render Cluster Hardware Document")
            renderForClusterHardware()
        }
    }

    void renderForLocalHardware(){
        //Benchmark GFlops
        log.info("Benchmarking FLOPS ...")
        //String gFlops = executeArrayFireScript()
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

        //write the batch_host_local.xml file with the benchmark results from above
        writeHostsXMLFileLocal(gFlops, cores, readSpeed, writeSpeed, space.toString())
    }

    void renderForClusterHardware(){
        List<String> listOfNodes = executeCommand(["sinfo"])
        String line = listOfNodes.stream().filter(it -> it.contains("debug*")).toArray().first()
        String nodeString = line.split(" ").last()
        //String nodeName = ""
        List<String> indexes = new ArrayList<>()
        if(nodeString.contains("[")){
            log.info("Muliple Compute Nodes")
            nodeString = nodeString.replace("[", ";").replace("]", ";")
            String nodeName = nodeString.split(";").first()
            String indexString = nodeString.split(";")[1]

            int startIndex = Integer.parseInt(indexString.split("-").first())
            int endIndex = Integer.parseInt(indexString.split("-")[1])
            for(int i = startIndex; i<=endIndex; i++){
                indexes.add(nodeName+i.toString())
            }
        }
            //only one compute node
        else{
            log.info("Single Compute Node")
            indexes.add(nodeString)
        }
        //log.info("Compute Node List: $nodeName")
        //indexes.forEach(it -> log.info(it))

        log.info("Benchmarking Master Node...")

        String local_gFlops = executeGFlopsBenchmark()//Get number of cores
        ArrayList<String> coresRaw = executeCommand(["getconf","_NPROCESSORS_ONLN"])
        String local_cores = coresRaw.first()

        //read speed fileio
        String rawReadSpeedData = executeSysbenchCommand(["/bin/bash", "-c", "sysbench --file-test-mode=seqrd fileio prepare;sysbench --file-test-mode=seqrd fileio run"], \
             "read, MiB/s", ":", 1)
        Double readSpeedDataMiB = rawReadSpeedData.toDouble()
        //convert to MBps
        String local_readSpeed = (readSpeedDataMiB * 1.048576).round(3).toString()

        //write speed fileio
        String rawWriteSpeedData = executeSysbenchCommand(["/bin/bash", "-c", "sysbench --file-test-mode=seqwr fileio prepare;sysbench --file-test-mode=seqwr fileio run"], \
             "written, MiB/s", ":", 1)
        Double writeSpeedDataMiB = rawWriteSpeedData.toDouble()
        //convert to MBps
        String local_writeSpeed = (writeSpeedDataMiB * 1.048576).round(3).toString()

        //disk size
        File file = new File("/")
        //Convert Byte to Gibibyte (GiB) by dividing by 1.074e+9
        int local_space = file.totalSpace/1.074e+9

        String ipCommand = "hostname -I"
        String ipResponse = executeCommand(["bash", "-c", ipCommand]).first()
        String ipAddress = ipResponse.split(" ").first()

        List<SlurmNode> nodes = new ArrayList<>()
        nodes.add(new SlurmNode("Master", SlurmNode.Role.MASTER, Double.parseDouble(local_gFlops), \
                    Integer.parseInt(local_cores), new Disk(local_readSpeed, local_writeSpeed, local_space.toString()), ipAddress))

        //add worker nodes
        for(node in indexes){
            SlurmNode slurmNode = getSlurmNode(node)
            nodes.add(slurmNode)
        }
        //benchmark network bandwidth
        log.info("Benchmarking Network Link ...")
        List<Double> bandwidths = new ArrayList<>()

        String bw = benchmarkNetworkBandwidth(nodes)
        nodes.forEach(it->log.info(it.toString()))
        log.info("Network-Bandwidth: $bw MBps")

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


    String executeGFlopsBenchmark(){
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

    void writeHostsXMLFileLocal(String gFlops, String cores, String readSpeed, String writeSpeed, String diskSize){
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

    void writeHostsLocal(XMLStreamWriter w, String gFlops, String cores, String readSpeed, String writeSpeed, String diskSize){
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

    void writeLinksLocal(XMLStreamWriter w){
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

    void writeRoutesLocal(XMLStreamWriter w){
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

    void writeLoopbackRoutesLocal(XMLStreamWriter w){
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

    SlurmNode getSlurmNode(String nodeName){
        log.info("Benchmarking Node: $nodeName ...")
        //GFlops docker run -e LINPACK_ARRAY_SIZE=150 h20180061/linpack
        String gFlopCommand = "srun -l --nodelist=$nodeName docker run h20180061/linpack"
        List<String> gFlopResponse = executeCommand(["bash", "-c", gFlopCommand])
        int count = gFlopResponse.stream()
                .filter(it -> it.contains("%"))
                .count()

        Double sum = gFlopResponse.stream()
                .filter(it -> it.contains("%"))
                .map(it->Double.parseDouble(it.split(" ").last()))
                .reduce(0.0, (x,y)-> x+y)

        Double avg = sum/count
        String gFlops = (avg/10000).round(3).toString()
        //log.info("GFLOPS: $gFlops")

        //cores: srun -l --nodelist=test-compute-0-0 getconf _NPROCESSORS_ONLN
        String coreCommand = "srun --nodelist=$nodeName getconf _NPROCESSORS_ONLN"
        String cores = executeCommand(["bash", "-c", coreCommand]).first()
        //log.info("CORES: $cores")


        //DISK
        String diskCommand = "srun --nodelist=$nodeName docker run severalnines/sysbench /bin/bash -c ; sysbench --file-test-mode=seqrd fileio prepare; sysbench --file-test-mode=seqrd fileio run; sysbench --file-test-mode=seqwr fileio run"
        List<String> diskResponse = executeCommand(["bash", "-c", diskCommand])
        //diskResponse.forEach(it-> log.info(it))

        //diskReadSpeed
        String readSpeed = diskResponse.stream()
                    .filter(it -> it.contains("read, MiB/s:"))
                    .map(it -> it.split(" ").last())
                    .map(it -> Double.parseDouble(it))
                    .filter(it -> it > 0)
                    .map(it -> (it*1.048576).toString()).toArray().first()
        //log.info("readSpeed; $readSpeed")

        //diskWriteSpeed
        String writeSpeed = diskResponse.stream()
                .filter(it -> it.contains("written, MiB/s:"))
                .map(it -> it.split(" ").last())
                .map(it -> Double.parseDouble(it))
                .filter(it -> it > 0)
                .map(it -> (it*1.048576).toString()).toArray().first()

        //log.info("writeSpeed: $writeSpeed")

        //diskSize
        String diskSizeCommand = "srun --nodelist=$nodeName df"
        List<String> diskSizeResponse = executeCommand(["bash", "-c", diskSizeCommand])
        String sizeLine = diskSizeResponse.stream()
                        .filter(it -> it.endsWith("/")).toArray().first()
        String sizeValuesOptional = sizeLine.split(" ").toList().stream()
                                    .filter(it -> it.isNumber())
                                    .findFirst()
        String sizeValue = ((Double.parseDouble(removeOptionalFromString(sizeValuesOptional))/1.024)/1000000).toString()
        //log.info(sizeValue)

        String ipCommand = "srun --nodelist=$nodeName hostname -I"
        String ipResponse = executeCommand(["bash", "-c", ipCommand]).first()
        String ipAddress = ipResponse.split(" ").first()



        new SlurmNode(nodeName, SlurmNode.Role.WORKER, Double.parseDouble(gFlops), Integer.parseInt(cores), \
                                                        new Disk(readSpeed, writeSpeed, sizeValue), ipAddress)

    }

    boolean checkIfSlurmConfigured(){
        File file = new File(System.getProperty("user.dir")+"/nextflow.config")
        Scanner scanner = new Scanner(file)
        int lineNum = 0
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            lineNum++
            if(line.contains("slurm")) {
                return true
            }
        }
        false
    }

    String benchmarkNetworkBandwidth(List<SlurmNode> nodes) {
        log.info("Benchmarking Network Link ...")
        List<Double> bandwidths = new ArrayList<>()
        for (node in nodes) {
            if (node.role == SlurmNode.Role.MASTER) continue
            String nodeName = node.name

            //startServer
            String serverScript = new File((System.getProperty("user.dir") + "/modules/nextflow/src/main/resources/slurm/iperfServer.sh")).toString()
            String serverCommand = "sbatch -w $nodeName $serverScript"
            executeCommand(["bash", "-c", serverCommand])

            //startClient
            //log.info("start client ...")
            String ip = node.ipAddress
            String clientCommand = "iperf -c $ip"
            List<String> response = executeCommand(["bash", "-c", clientCommand])

            //parse the bandwidth value
            String rawBandwidth = response.stream()
                    .filter(it -> it.contains("sec"))
                    .map(it -> it.split("Bytes")[1])
                    .map(it -> it.replace(" ", ""))
                    .map(it -> it.replace("bits/sec", "")).toArray().first()

            if (rawBandwidth.contains("G")) {
                Double mb = (Double.parseDouble(rawBandwidth.replace("G", ""))) * 128
                bandwidths.add(mb)
            } else {
                Double mb = (Double.parseDouble(rawBandwidth.replace("M", ""))) / 8
                bandwidths.add(mb)
            }

            Double bandwidthSum = bandwidths.stream().reduce(0, (a, b) -> a + b)
            String bandwidth = (bandwidthSum / bandwidths.size()).toString()
            bandwidth
        }
    }

    static String removeOptionalFromString(String optional){
        String split1 = optional.split("Optional")[1]
        return split1.substring(1, split1.length()-1)
    }


    class SlurmNode{
        String name
        enum Role{
            MASTER,
            WORKER
        }
        Role role
        Double gFlops
        Integer cores
        Disk disk
        String ipAddress

    SlurmNode(String name, Role role, Double gFlops, Integer cores, Disk disk, String ipAddress){
        this.name = name
        this.role = role
        this.gFlops = gFlops
        this.cores = cores
        this.disk = disk
        this.ipAddress = ipAddress
    }

    boolean setDisk(Double readSpeed, Double writeSpeed, Double size){
        if(this.disk == null){
            this.disk = new Disk(readSpeed, writeSpeed, size)
            return true
        }
        return false
    }

    String toString(){
        return "name: $name, role: $role, gFlops: $gFlops, cores: $cores, disk: $disk, ipAddress: $ipAddress"
    }

}

class Disk{
    String readSpeed
    String writeSpeed
    String size

    Disk(String readSpeed, String writeSpeed, String size){
        this.readSpeed = readSpeed
        this.writeSpeed = writeSpeed
        this.size = size
    }

    String toString(){
        "Disk(readSpeed: $readSpeed, writeSpeed: $writeSpeed, size: $size)"
    }

}

}


