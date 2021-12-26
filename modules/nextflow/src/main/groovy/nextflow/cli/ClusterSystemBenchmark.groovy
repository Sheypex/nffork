package nextflow.cli

import groovy.util.logging.Slf4j
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseJson

@Slf4j
class ClusterSystemBenchmark implements SystemBenchmark{
    private K8sClient api

    ClusterSystemBenchmark(K8sClient client){
        this.api = client
    }

    @Override
    void renderHardwareDocument() {

        log.info("render Cluster Hardware")
        List<K8sNode> nodes = getNodes()


        for(node in nodes){
            log.info(node.toString())
//            node.gFlops = benchmarkGFlops(node)
//            node.cores = getCores(node)
//            node.disk = getDisk(node)
        }
//        List<String> networkLink = benchmarkNetworkLink()
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

    List<K8sNode.Roles> getK8sRolesFromString(String str){
        ArrayList<K8sNode.Roles> ret = new ArrayList<>()
        if(str.contains("master")) ret.add(K8sNode.Roles.MASTER)
        if(str.contains("control-plane")) ret.add(K8sNode.Roles.CONTROL_PLANE)
        else if(str.contains("worker")) ret.add(K8sNode.Roles.WORKER)
        else ret.add(K8sNode.Roles.NONE)
        return ret
    }

    Double benchmarkGFlops(K8sNode node){
        //TODO
        return null
    }


    void renderForClusterHardware(){
        log.info("RenderForClusterHardware invoked")

        //get all nodes
//        ArrayList<String> resp = executeCommand([ "kubectl", "get", "nodes"])
//        resp.forEach(it -> log.info(it))
//        ArrayList<String> splitted = resp.first().split(" ")
//        log.info("size after filtering: " + splitted.stream().filter(str -> str!= "").count())
//
        def response = createSingleContainerPod("benchmark-34", "default", \
 "example34", "arrayfire/arrayfire", ["/bin/bash"] as String[])
        log.info("RESPONSE:" + response.toString())

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

    K8sResponseJson createSingleContainerPod(String name, String namespace, String containerName,  \
  String containerImage, String[] command){

        LinkedHashMap<String, Serializable> request = new LinkedHashMap<>()
        request.put("apiVersion", "v1")
        request.put("kind", "Pod")

        //meta data
        LinkedHashMap<String, Serializable> metaData = new LinkedHashMap<>()
        metaData.put("name", name)
        metaData.put("namespace", namespace)
        //meta data labels
        LinkedHashMap<String, Serializable> metaDataLabels = new LinkedHashMap<>()
        metaDataLabels.put("app", "nextflow")
        metaDataLabels.put("runName", name)

        metaData.put("labels", metaDataLabels)

        request.put("metadata", metaData)

        //spec
        LinkedHashMap<String, Serializable> spec = new LinkedHashMap<>()
        //containers
        LinkedHashMap<String, Serializable> containers = new LinkedHashMap<>()
        containers.put("name", containerName)
        containers.put("image", containerImage)

        ArrayList<String> commandContainer = new ArrayList<>()
        for(entry in command){
            commandContainer.add(entry)
        }
        containers.put("command", commandContainer)

        ArrayList<LinkedHashMap> containerList = new ArrayList<>()
        containerList.add(containers)
        spec.put("containers", containerList)

        request.put("spec", spec)

        log.info(request.toString())
        return api.podCreate(request, null)
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


        class Disk{
            Double readSpeed
            Double writeSpeed
            Double size

            Disk(Double readSpeed, Double writeSpeed, Double size){
                this.readSpeed = readSpeed
                this.writeSpeed = writeSpeed
                this.size = size
            }

        }

    }
}
