package nextflow.dag

import nextflow.processor.TaskId
import nextflow.trace.TraceRecord
import groovy.text.GStringTemplateEngine
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.logging.Logger
import java.nio.file.Path
import groovy.transform.PackageScope
@Slf4j
class DAXRenderer {

    private DAG dag
    private Map<TaskId,TraceRecord> records
    private int tasksLength

    DAXRenderer (dag, records){
        this.dag = dag
        this.records = records
        this.tasksLength = records.size()
    }

    void renderDAX(){


        log.info("")
        log.info("---------------------------------------------------")
        log.info("renderDAX in DAXRenderer aufgerufen")
        log.info("---------------------------------------------------")
        log.info("records length: $tasksLength")
        log.info("")
        for (r in records){
            log.info(r.toString())
            log.info("")
        }
        log.info("")
        log.info("---------------------------------------------------")
        log.info("")
        log.info("dag exists: " +dag.toString())

        log.info("")
        log.info("---------------------------------------------------")
        def nodes = dag.getVertices()
        log.info("nodes.size() = "+nodes.size())
        for (n in nodes){
            log.info("node : "+n.toString())
        }

        log.info("")
        log.info("---------------------------------------------------")
        def edges = dag.getEdges()
        log.info("edges.size() = "+edges.size())
        for (e in edges){
            log.info("edge : "+ e.toString())
        }


    }

}
