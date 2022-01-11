package nextflow.cli

/**
 * Renders a simgrid XML file describing executing hardware.
 * Can be used for simulations in WRENCH (https://wrench-project.org/)
 * @author Frederic Risling
 */
interface SystemBenchmark {

    /**
     * renders the hardware document describing the executing hardware
     * @author Frederic Risling
     */
    void renderHardwareDocument()
}