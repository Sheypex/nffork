package nextflow.dag

import nextflow.processor.TaskId
import nextflow.trace.TraceRecord
import groovy.text.GStringTemplateEngine
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.logging.Logger
import java.nio.file.Path
import groovy.transform.PackageScope
@Slf4j

/**
 * Render the DAG in .dax Format representing the Workflow.
 * Renders the minimal DAG in .dax Format for simulations in Wrench (https://wrench-project.org/)
 * @author Frederic Risling
 */
class DAXRenderer {

    /**
     * The current DAG
     */
    private DAG dag

    /**
     * All the trace records of the current execution.
     * Contains the runtime for the tasks
     */
    private Map<TaskId,TraceRecord> records

    /**
     * The path where the DAG will be saved
     */
    private Path path

    /**
     * Finals for creating the .dax file
     */
    private static final String XMLNS = "http://pegasus.isi.edu/schema/DAX"
    private static final String XMLNS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
    private static final String XSI_LOCATION = XMLNS + " http://pegasus.isi.edu/schema/dax-2.1.xsd"
    private static final String VERSION ="2.1"


    /**
     * Create a DAXRenderer
     * @param dag
     * @param records
     */
    DAXRenderer (dag, records, path){
        this.dag = dag
        this.records = records
        this.path = path
    }

    void renderDAX(){


        log.info("")
        log.info("---------------------------------------------------")
        log.info("renderDAX in DAXRenderer aufgerufen")
        log.info("---------------------------------------------------")
        //log.info("records length: $tasksLength")
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
        //XML File erstellen
        final Charset charset = Charset.defaultCharset()
        Writer bw = Files.newBufferedWriter(path, charset)
        final XMLOutputFactory xof = XMLOutputFactory.newFactory()
        final XMLStreamWriter w = xof.createXMLStreamWriter(bw)
        w.writeStartDocument(charset.displayName(), "1.0")
        w.writeStartElement("adag")
        w.writeAttribute("xmlns", XMLNS)
        w.writeEndElement()
        w.writeEndDocument()
        w.flush()
        bw.flush()
        bw.close()

    }

}
