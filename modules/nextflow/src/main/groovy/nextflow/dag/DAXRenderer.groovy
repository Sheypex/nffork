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
        //println("Hello")
        log.info("")
        log.info("---------------------------------------------------")
        log.info("renderDAX in DAXRenderer aufgerufen")
        log.info("records length: $tasksLength")
        log.info("dag exists: " +dag.toString())

    }

}
