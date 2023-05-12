/*
 * Copyright 2020-2022, Seqera Labs
 * Copyright 2013-2019, Centre for Genomic Regulation (CRG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.trace

import nextflow.dag.DAXRenderer
import nextflow.processor.TaskId

import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.dag.CytoscapeHtmlRenderer
import nextflow.dag.DAG
import nextflow.dag.DagRenderer
import nextflow.dag.DotRenderer
import nextflow.dag.GexfRenderer
import nextflow.dag.GraphvizRenderer
import nextflow.dag.MermaidRenderer
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
/**
 * Render the DAG document on pipeline completion using the
 * format specified by the user
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class GraphObserver implements TraceObserver {

    static public final String DEF_FILE_NAME = "dag-${TraceHelper.launchTimestampFmt()}.dot"

    private Path file

    private DAG dag

    private String name

    private String format

    boolean overwrite

    //My additions:
    private Session session
    static final public int DEF_MAX_TASKS =10_000
    final private Map<TaskId, TraceRecord> records = new LinkedHashMap<>()
    private int maxTasks = DEF_MAX_TASKS
    private ResourcesAggregator aggregator

    String getFormat() { format }

    String getName() { name }

    GraphObserver( Path file ) {
        assert file
        this.file = file
        this.name = file.baseName
        this.format = file.getExtension().toLowerCase() ?: 'dot'
    }

    @Override
    void onFlowCreate(Session session) {
        this.dag = session.dag
        this.session = session
        this.aggregator = new ResourcesAggregator(session)
        // check file existance
        final attrs = FileHelper.readAttributes(file)
        if( attrs ) {
            if( overwrite && (attrs.isDirectory() || !file.delete()) )
                throw new AbortOperationException("Unable to overwrite existing DAG file: ${file.toUriString()}")
            else if( !overwrite )
                throw new AbortOperationException("DAG file already exists: ${file.toUriString()} -- enable `dag.overwrite` in your config file to overwrite existing DAG files")
        }
    }

    @Override
    void onFlowComplete() {
        // -- normalise the DAG
        dag.normalize()
        // -- render it to a file
        createRender().renderDocument(dag,file)
    }

    @PackageScope
    DagRenderer createRender() {
        if( format == 'dot' )
            new DotRenderer(name)

        else if( format == 'html' )
            new CytoscapeHtmlRenderer()

        else if( format == 'gexf' )
            new GexfRenderer(name)

        else if ( format == 'dax')
            new DAXRenderer(records, session)

        else if( format == 'mmd' )
            new MermaidRenderer()

        else
            new GraphvizRenderer(name, format)
    }


    @Override
    void onProcessCreate(TaskProcessor process) {

    }


    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        log.trace "Trace report - submit process > $handler"
        synchronized (records) {
            records[ trace.taskId ] = trace
        }
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        log.trace "Trace report - start process > $handler"
        synchronized (records) {
            records[ trace.taskId ] = trace
        }
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        log.trace "Trace report - complete process > $handler"
        if( !trace ) {
            log.debug "WARN: Unable to find trace record for task id=${handler.task?.id}"
            return
        }
        synchronized (records) {
            records[ trace.taskId ] = trace
            aggregate(trace)
        }
    }

    @Override
    boolean enableMetrics() {
        return true
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        log.trace "Trace report - cached process > $handler"

        // event was triggered by a stored task, ignore it
        if( trace == null ) {
            return
        }

        // remove the record from the current records
        synchronized (records) {
            records[ trace.taskId ] = trace
            aggregate(trace)
        }
    }

    /**
     * Aggregates task record for each process in order to render the
     * final execution stats
     *
     * @param record A {@link TraceRecord} object representing a task executed
     */
    protected void aggregate(TraceRecord record) {
        aggregator.aggregate(record)
    }
}
