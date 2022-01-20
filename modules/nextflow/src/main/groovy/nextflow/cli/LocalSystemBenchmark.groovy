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
        //get all computing nodes
        List<String> listOfNodes = executeCommand(["sinfo"])
        List<String> lines = listOfNodes.stream()
                                .filter(it -> it.contains("debug*"))
                                .filter(it -> it.contains("mix") || it.contains("idle")).toArray()
        //save indexes of nodes
        List<String> indexes = new ArrayList<>()
        //iterate through all nodes in idle or mix state
        for(line in lines){
            String nodeString = line.split(" ").last()
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
            //only one compute node in line
            else{
                log.info("Found Compute Node")
                indexes.add(nodeString)
            }
        }


        //Benchmark master node (local)
        log.info("Benchmarking Master Node...")
        //gflops master
        String local_gFlops = executeGFlopsBenchmark()//Get number of cores
        //cores master
        ArrayList<String> coresRaw = executeCommand(["getconf","_NPROCESSORS_ONLN"])
        String local_cores = coresRaw.first()
        //disk master
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

        //ip address for master
        String ipCommand = "hostname -I"
        String ipResponse = executeCommand(["bash", "-c", ipCommand]).first()
        String ipAddress = ipResponse.split(" ").first()

        //add master node to node list
        List<SlurmNode> nodes = new ArrayList<>()
        nodes.add(new SlurmNode("Master", SlurmNode.Role.MASTER, Double.parseDouble(local_gFlops), \
                    Integer.parseInt(local_cores), new Disk(local_readSpeed, local_writeSpeed, local_space.toString()), ipAddress))

        //add worker nodes
        for(node in indexes){
            SlurmNode slurmNode = getSlurmNode(node)
            nodes.add(slurmNode)
        }
        //benchmark network bandwidth
        String bw = benchmarkNetworkBandwidth(nodes)
        //benchmark network latency
        String latency = benchmarkNetworkLatency(nodes)
        // print all nodes
        nodes.forEach(it->log.info(it.toString()))
        //print network bandwidth
        log.info("Network-Bandwidth: $bw MBps")
        //print network latency
        log.info("Latency: $latency ms")

        //write cluster host XML file
        writeHostsXMLFileCluster(nodes, bw, latency)

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

        //Gflops
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

        //cores
        String coreCommand = "srun --nodelist=$nodeName getconf _NPROCESSORS_ONLN"
        String cores = executeCommand(["bash", "-c", coreCommand]).first()

        //DISK
        String diskCommand = "srun --nodelist=$nodeName docker run severalnines/sysbench /bin/bash -c ; sysbench --file-test-mode=seqrd fileio prepare; sysbench --file-test-mode=seqrd fileio run; sysbench --file-test-mode=seqwr fileio run"
        List<String> diskResponse = executeCommand(["bash", "-c", diskCommand])

        //diskReadSpeed
        String readSpeed = diskResponse.stream()
                    .filter(it -> it.contains("read, MiB/s:"))
                    .map(it -> it.split(" ").last())
                    .map(it -> Double.parseDouble(it))
                    .filter(it -> it > 0)
                    .map(it -> (it*1.048576).toString()).toArray().first()

        //diskWriteSpeed
        String writeSpeed = diskResponse.stream()
                .filter(it -> it.contains("written, MiB/s:"))
                .map(it -> it.split(" ").last())
                .map(it -> Double.parseDouble(it))
                .filter(it -> it > 0)
                .map(it -> (it*1.048576).toString()).toArray().first()

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
        String bandwidth
        for (node in nodes) {
            if (node.role == SlurmNode.Role.MASTER) continue
            String nodeName = node.name

            //start server
            String serverScript = new File((System.getProperty("user.dir") + "/modules/nextflow/src/main/resources/slurm/iperfServer.sh")).toString()
            String serverCommand = "sbatch -w $nodeName $serverScript"
            executeCommand(["bash", "-c", serverCommand])

            //startClient
            String ip = node.ipAddress
            String clientCommand = "iperf -c $ip"
            List<String> response = executeCommand(["bash", "-c", clientCommand])

            //added so that there is enough time to finish previous execution
            Thread.sleep(10000)

            String rawBandwidth = ""
            //parse the bandwidth value
            try{
                rawBandwidth = response.stream()
                        .filter(it -> it.contains("sec"))
                        .map(it -> it.split("Bytes")[1])
                        .map(it -> it.replace(" ", ""))
                        .map(it -> it.replace("bits/sec", "")).toArray().first()
            }
            catch (Exception e){
                log.info("Could not benchmark network link for: $nodeName Cause: node is in state: mixed")
            }

            if (rawBandwidth.contains("G")) {
                Double mb = (Double.parseDouble(rawBandwidth.replace("G", ""))) * 128
                bandwidths.add(mb)
            } else if (rawBandwidth.contains("M")) {
                Double mb = (Double.parseDouble(rawBandwidth.replace("M", ""))) / 8
                bandwidths.add(mb)
            }
        }
        Double bandwidthSum = bandwidths.stream().reduce(0, (a, b) -> a + b)
        bandwidth = (bandwidthSum / bandwidths.size()).toString()
        bandwidth
    }

    String benchmarkNetworkLatency(List<SlurmNode>nodes){
        List<Double> pings = new ArrayList<>()
        for (node in nodes){
            String ip = node.ipAddress
            String command = "timeout 10 ping $ip"
            List<String> response = executeCommand(["bash", "-c", command])
            List<Double> ping = response.stream()
                    .filter(it -> !it.contains("PING"))
                    .map(it -> it.split("time=")[1])
                    .map(it -> it.split(" ")[0])
                    .map(it -> Double.parseDouble(it)).toArray()
            Double sum = ping.stream().reduce(0, (a,b) -> a+b)
            pings.add(sum/ping.size())
        }
        Double totalSum = pings.stream().reduce(0, (a,b) -> a+b)
        (totalSum/pings.size()).toString()
    }

    void writeHostsXMLFileCluster(List<SlurmNode> nodes, String bandwidth, String latency){
        log.info("Writing Cluster Hardware file: cluster_hosts.xml")

        Path path = new File((System.getProperty("user.dir")+"/cluster_hosts.xml")).toPath()
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
        writeHostsCluster(w, nodes)

        //write links
        writeLinksCluster(w, nodes, bandwidth, latency)

        //write network routes
        writeRoutesCluster(w, nodes)

        //write loopback routes
        //writeLoopbackRoutesCluster(w, nodes)

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

    void writeHostsCluster(XMLStreamWriter w, List<SlurmNode> nodes){
        for(int i = 1; i <= nodes.size(); i++){
            //<host id="HostXY" speed="xyzGf" core="xy">
            String hostName = "Host" + i.toString()
            w.writeCharacters("\n\t\t")
            w.writeStartElement("host")
            w.writeAttribute("id", hostName)
            w.writeAttribute("speed", nodes[i-1].gFlops.toString() + "Gf")
            w.writeAttribute("core", nodes[i-1].cores.toString())
            //<disk id="large_disk" read_bw="xyzMBps" write_bw="xyzMBps">
            w.writeCharacters("\n\t\t\t")
            w.writeStartElement("disk")
            w.writeAttribute("id", "large_disk")

            //WRENCH requires: readSpeed==writeSpeed --> Thus, we use the average of both
            String avgDiskSpeed = ((nodes[i-1].disk.readSpeed.toDouble() + nodes[i-1].disk.writeSpeed.toDouble())/2).round(3).toString()
            w.writeAttribute("read_bw", avgDiskSpeed+"MBps")
            w.writeAttribute("write_bw", avgDiskSpeed+"MBps")

            //<prop id="size" value="XYZGiB"/>
            w.writeCharacters("\n\t\t\t\t")
            w.writeStartElement("prop")
            w.writeAttribute("id", "size")
            w.writeAttribute("value", (nodes[i-1].disk.size+"GiB"))
            w.writeEndElement()
            //<prop id="mount" value="/"/>
            w.writeCharacters("\n\t\t\t\t")
            w.writeStartElement("prop")
            w.writeAttribute("id", "mount")
            w.writeAttribute("value","/")
            w.writeEndElement()

            //</disk>/Users/frederic.risling/Desktop/cluster_hosts.xml
            w.writeCharacters("\n\t\t\t")
            w.writeEndElement()

            //host
            w.writeCharacters("\n\t\t")
            w.writeEndElement()

        }

    }

    void writeLinksCluster(XMLStreamWriter w, List<SlurmNode> nodes, String bandwidth, String latency){
        w.writeCharacters("\n\n\t\t")
        w.writeComment(" A network link ")
        //<link id="network_link" bandwidth="5000GBps" latency="0us"/>
        w.writeCharacters("\n\t\t")
        w.writeStartElement("link")
        w.writeAttribute("id", "network_link")
        //catch the case where the networkBandwidth couldnt be parsed
        if(bandwidth == "NaN"){
            w.writeAttribute("bandwidth",  "12500MBps")
        }
        else{
            w.writeAttribute("bandwidth",  Double.parseDouble(bandwidth).round(3).toString()+"MBps")
        }
        w.writeAttribute("latency", Double.parseDouble(latency).round(3).toString()+"ms")
        w.writeEndElement()

        for(int i = 1; i <= nodes.size(); i++){
            //<!-- Host1's local "loopback" link...-->
            String str_i = i.toString()
            w.writeCharacters("\n\t\t")
            w.writeComment(" Host$str_i's local \"loopback\" link...")
            //<link id="loopback_Host1" bandwidth="1000EBps" latency="0us"/>
            w.writeCharacters("\n\t\t")
            w.writeStartElement("link")
            w.writeAttribute("id", "loopback_Host$str_i")
            w.writeAttribute("bandwidth", "1000EBps")
            w.writeAttribute("latency", "0us")
            w.writeEndElement()
        }

    }

    void writeRoutesCluster(XMLStreamWriter w, List<SlurmNode> nodes){
        w.writeCharacters("\n\n\n\t\t")
        //<!-- The network link connects all hosts together -->
        w.writeComment(" The network link connects all hosts together ")

        List<int[]> tuples = generate(nodes.size(), 2)
        tuples.forEach(it -> log.info(it.toString()))

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


    }

    static String removeOptionalFromString(String optional){
        String split1 = optional.split("Optional")[1]
        return split1.substring(1, split1.length()-1)
    }

    //https://www.baeldung.com/java-combinations-algorithm
    List<int[]> generate(int n, int r) {
        List<int[]> combinations = new ArrayList<>()
        helper(combinations, new int[r], 0, n-1, 0)
        return combinations;
    }

    private void helper(List<int[]> combinations, int[] data, int start, int end, int index) {
        if (index == data.length) {
            int[] combination = data.clone()
            combinations.add(combination)
        } else if (start <= end) {
            data[index] = start
            helper(combinations, data, start + 1, end, index + 1)
            helper(combinations, data, start + 1, end, index)
        }
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
        "Disk(readSpeed: $readSpeed, writeSpeed: $writeSpeed, size: $size"
    }

}

}


