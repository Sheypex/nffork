package nextflow.cli

/**
 * Renders a simgrid XML file describing executing hardware.
 * Can be used for simulations in WRENCH (https://wrench-project.org/)
 * @author Frederic Risling
 */
interface SystemBenchmark {

    /**
     * format of the file (!DOCTYPE)
     */
    final String SIMGRID_DTD = "\"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\""


    /**
     * Simgrid version of the file
     */
    final String VERSION = "4.1"


    /**
     * renders the hardware document describing the executing hardware
     * @author Frederic Risling
     */
    void renderHardwareDocument()

}