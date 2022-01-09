package nextflow.cli

import groovy.util.logging.Slf4j
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseJson

import java.nio.file.Path
import java.security.SecureRandom
import java.util.regex.Pattern
import java.util.stream.Stream

@Slf4j
class ClusterSystemBenchmark implements SystemBenchmark{
    private K8sClient api

    ClusterSystemBenchmark(K8sClient client){
        this.api = client
    }

    @Override
    void renderHardwareDocument() {

        log.info("Benchmarking Cluster Hardware...")
        List<K8sNode> nodes = getNodes()
        nodes.forEach(it->log.info("$it.name: $it.roles"))
        //benchmark each node
        for(node in nodes){
            submitGFlopsPod(node)
            log.info("Benchmarking node: " + node.name + " ...")
            Double gFlops = parseGFlopsForNode(node)
            node.setgFlops(gFlops)
            node.cores = getCores(node)
            node.disk = getDisk(node)
            log.info(node.toString())
        }
        //benchmark network-bandwith
        String networkBandwith = benchmarkNetworkLink()
        log.info("Network Bandwith: $networkBandwith GBps")

    }

    List<K8sNode> getNodes(){
        ArrayList<K8sNode> nodes = new ArrayList<>()
        ArrayList<String> output = executeCommand(["kubectl", "get", "nodes"])
        for(out in output.drop(1)){
            ArrayList<String> split = out.split(" ")
            ArrayList<String> filtered = split.stream().filter(it -> it != "").toArray()
            String nodeName = filtered.get(0)
            Boolean nodeReady = filtered.get(1) == "Ready" ? true : false
            ArrayList<K8sNode.Roles> roles = getK8sRolesFromString(filtered.get(2))
            K8sNode node = new K8sNode(nodeName, nodeReady, roles)
            nodes.add(node)
        }
        nodes
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

    List<K8sNode.Roles> getK8sRolesFromString(String str){
        ArrayList<K8sNode.Roles> ret = new ArrayList<>()
        if(str.contains("master")) ret.add(K8sNode.Roles.MASTER)
        if(str.contains("control-plane")) ret.add(K8sNode.Roles.CONTROL_PLANE)
        else if(str.contains("worker")) ret.add(K8sNode.Roles.WORKER)
        else ret.add(K8sNode.Roles.NONE)
        return ret
    }

    void submitGFlopsPod(K8sNode node){

        Map<String, Serializable> request = new LinkedHashMap<>()
        String apiVersion = "v1"
        String kind = "Pod"
        //metaData
        String name = "benchmarkgflops-"+ node.name
        String namespace = "default"
        request.putAll(makePodHead(apiVersion, kind, name, namespace))

        //check if pod already exists
        if(executeCommand(["kubectl", "get", "pod", name]).size()>1){
            return
        }

        //create spec
        //create container
        Map<String, Serializable> container = defineContainer("/bin/sh", ["-c", "cd build ; cd examples; cd benchmarks ; ./blas_cpu"],
            "arrayfire/arrayfire", "arrayfire-"+node.name, "5722Mi", 2)
        ArrayList<Map<String, Serializable>> containerList = new ArrayList<>()
        containerList.add(container)

        Map<String, Serializable> spec = new LinkedHashMap<>()
        spec.put("containers", containerList)
        spec.put("restartPolicy", "Never")
        spec.put("nodeName", node.name)

        //add spec to req
        request.put("spec", spec)

        //create pod
        api.podCreate(request, null)
    }

    Double parseGFlopsForNode(K8sNode node){
        Double gFlops
        String gFlopsPod = "benchmarkgflops-"+ node.name
        while(!executeCommand(["kubectl", "get", "pod", gFlopsPod]).get(1).contains("Completed")){
            Thread.sleep(10000)
        }
        try{
            List<String> reads = executeCommand(["kubectl", "logs", gFlopsPod])
            String peak = reads.last()
            String flops = peak.split(" ")[3]
            gFlops = Double.parseDouble(flops)
        }
        catch (Exception e){
            e.printStackTrace()
            log.warn("WARN: Could not benchmark GFlops for node: $node.name; \n" +
                    " Possible causes: node unavailable or memory/cpu unsifficient")
        }
        gFlops
    }

    Map<String, Serializable> makePodHead(String apiVersion, String kind, String name, String namespace){
        LinkedHashMap<String, Serializable> ret = new LinkedHashMap<>()
        ret.put("apiVersion", apiVersion)
        ret.put("kind", kind)
        //metaData
        LinkedHashMap<String, Serializable> metaData = new LinkedHashMap<>()
        metaData.put("name", name)
        metaData.put("namespace", namespace)
        //add metaData to Head
        ret.put("metadata", metaData)
        //return full pod head for yaml file
        ret
    }

    Map<String, Serializable> defineContainer(String command, List<String> args, String image, \
        String containerName, String memory, int cpu){

        LinkedHashMap<String, Serializable> containerProperties = new LinkedHashMap<>()
        List<String> commandAsList = new ArrayList<>()
        commandAsList.add(command)
        containerProperties.put("command", commandAsList)
        containerProperties.put("args", args)
        containerProperties.put("image", image)
        containerProperties.put("name", containerName)

        //mem and cpu
        LinkedHashMap<String, Serializable> memAndCpu = new LinkedHashMap<>()
        memAndCpu.put("memory", memory)
        memAndCpu.put("cpu", cpu)
        //resources limits
        LinkedHashMap<String, Serializable> limit = new LinkedHashMap<>()
        limit.put("limits", memAndCpu)
        //resources requests
        LinkedHashMap<String, Serializable> request = new LinkedHashMap<>()
        request.put("requests", memAndCpu)

        limit.putAll(request)
        containerProperties.put("resources", limit)

        containerProperties
    }

    int getCores(K8sNode node){
        String command = "kubectl describe node " + node.name  + " | grep 'cpu'"
        List<String> response = executeCommand(["bash", "-c", command])
        int cores
        if(!response.get(1).contains("m")){
            ArrayList<String> split = response.get(1).split(" ")
            cores = split.stream()
                    .filter(it -> it.isNumber())
                    .map(it-> Integer.parseInt(it))
                    .toArray()
                    .first()
            if(cores<1) cores = 1
        }
        else{
            ArrayList<String> split = response.get(1).split(" ")
            String core = split.stream()
                .filter(it -> it.contains("m"))
                .map(it -> Integer.parseInt(it.split("m")[0]))
                .findFirst()
                .toString().split("Optional")[1].substring(1).split("]")[0]
            Double core_raw = Integer.parseInt(core)/1000
            cores = core_raw.round(0).toInteger()
            if(cores<1) cores = 1
        }
        cores
    }

    Disk getDisk(K8sNode node){
        //getDiskSpace
        String command = "kubectl describe node " + node.name  + " | grep 'ephemeral-storage'"
        List<String> response = executeCommand(["bash", "-c", command])
        String diskSpace = ""
        try{
            diskSpace = response.first().split(":")[1].replace(" ", "")+"B"
        }
        catch (ArrayIndexOutOfBoundsException){
            log.info("node $node.name has no disk")
        }
        //if there is no disk return no disk
        if(diskSpace.length()<4) return null

        //benchmark readSpeed of Disk
        submitDiskBenchmarkPod(node)
        Tuple<String> readAndWriteSpeed = parseDiskSpeed(node)
        new Disk(readAndWriteSpeed[0], readAndWriteSpeed[1], diskSpace)

    }


    void submitDiskBenchmarkPod(K8sNode node){

        Map<String, Serializable> request = new LinkedHashMap<>()

        String apiVersion = "v1"
        String kind = "Pod"
        //metaData
        String name = "benchmark-disk-speed-"+ node.name
        String namespace = "default"
        request.putAll(makePodHead(apiVersion, kind, name, namespace))

        //check if pod already exists
        if(executeCommand(["kubectl", "get", "pod", name]).size()>1){
            return
        }

        //create spec
        //create container
        String command = "sysbench --file-test-mode=seqrd fileio prepare; sysbench --file-test-mode=seqrd fileio run; sysbench --file-test-mode=seqwr fileio run;"
        Map<String, Serializable> container = defineContainer("/bin/sh", ["-c", command],
                "severalnines/sysbench", "sysbench-benchmark-speed-"+node.name, "5722Mi", 2)
        ArrayList<Map<String, Serializable>> containerList = new ArrayList<>()
        containerList.add(container)

        Map<String, Serializable> spec = new LinkedHashMap<>()
        spec.put("containers", containerList)
        spec.put("restartPolicy", "Never")
        spec.put("nodeName", node.name)

        //add spec to req
        request.put("spec", spec)

        //create pod
        api.podCreate(request, null).toString()
    }

    Tuple<String> parseDiskSpeed(K8sNode node){
        String readSpeed
        String writeSpeed
        String diskPod = "benchmark-disk-speed-"+ node.name

        while(!executeCommand(["kubectl", "get", "pod", diskPod]).get(1).contains("Completed")){
            Thread.sleep(5000)
        }
        try{
            //parse readSpeed
            String command = "kubectl logs " + diskPod  + " | grep 'read, MiB/s'"
            List<String> reads = executeCommand(["bash", "-c", command])
            readSpeed = reads.stream().map(it -> it.split(":")[1].replace(" ", ""))
                    .map(it -> Double.parseDouble(it))
                    .filter(it -> it > 0)
                    .map(it -> (it*1.048576).toString())
                    .toArray()
                    .first()

            //parse writeSpeed
            String command2 = "kubectl logs " + diskPod  + " | grep 'written, MiB/s'"
            List<String> writes = executeCommand(["bash", "-c", command2])
            writeSpeed = writes.stream().map(it -> it.split(":")[1].replace(" ", ""))
                    .map(it -> Double.parseDouble(it))
                    .filter(it -> it > 0)
                    .map(it -> (it*1.048576).toString())
                    .toArray()
                    .first()
        }
        catch (Exception e){
            e.printStackTrace()
            log.warn("WARN: Could not benchmark GFlops for node: $node.name; \n" +
                    " Possible causes: node unavailable or memory/cpu unsifficient")
        }
        return new Tuple<String>(readSpeed, writeSpeed)
    }

    String benchmarkNetworkLink(){

        String path = System.getProperty("user.dir")
        log.info("Path: " + path)
        //benchmark network
        String command = "cd $path; git clone https://github.com/Pharb/kubernetes-iperf3.git; cd kubernetes-iperf3; ./iperf3.sh"
        List<String> response = executeCommand(["bash", "-c", command])
        //delete benchmark files
        String command2 = "cd $path; rmdir kubernetes-iperf3"
        executeCommand(["bash", "-c", command2])

        int averagePerConnection = response.stream()
                                .filter(it-> it.contains("sender") || it.contains("receiver"))
                                .count()
        Double networkBandwith = response.stream()
                                .filter(it-> it.contains("sender") || it.contains("receiver"))
                                .map(it -> it.find(".([0-9]?)[0-9].([0-9]?)[0-9] [MG]bits/sec"))
                                .map(it -> it.split("bits/sec")[0])
                                .map(it -> it.contains("M")? Double.parseDouble(it.substring(0, it.size()-2))/1000 : Double.parseDouble(it.substring(0, it.size()-2)))
                                .reduce(0, (prev, it) -> prev+it)/averagePerConnection

        //GByte per Second
        (networkBandwith/8).round(3).toString()
    }


    class K8sNode{
        String name
        Boolean ready
        enum Roles{
            CONTROL_PLANE,
            MASTER,
            WORKER,
            NONE
        }
        List<Roles> roles

        Double gFlops
        Integer cores
        Disk disk

        K8sNode(String name, Boolean ready, List<Roles> roles){
            this.name = name
            this.ready = ready
            this.roles = roles
            this.gFlops = null
            this.cores = null
            this.disk = null
        }

        boolean setGFlops(Double gFlops){
            if(this.gFlops == null){
                this.gFlops = gFlops
                return true
            }
            return false

        }

        boolean setCores(int cores){
            if(this.cores == null){
                this.cores = cores
                return true
            }
            return false
        }

        boolean setDisk(Double readSpeed, Double writeSpeed, Double size){
            if(this.disk == null){
                this.disk = new Disk(readSpeed, writeSpeed, size)
                return true
            }
            return false
        }

        String toString(){
            return "name: $name, ready: $ready, roles: $roles, gFlops: $gFlops, cores: $cores, disk: $disk"
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
