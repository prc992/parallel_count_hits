process create_results_dir{
    output:
    val ('out_put')
    
    script:
    """
    mkdir -p $params.results &&
    mkdir -p $params.agg_results
    """
}


process executa_count_hits{
    //Docker Image
    maxForks params.max_parallel_process
    queue = "$params.queue"
    container = "quay.io/biocontainers/pybedtools:0.9.0--py38he0f268d_2"
    tag "$encode_file"

    publishDir "$params.results", mode : 'copy'

    input:
    path (py_program)
    path (ref_file)
    each path (encode_file)
    val (_)
    
    output:
    path ('*.csv')

    script:
    """
    python $py_program $ref_file $encode_file
    """
}

process executa_merge{
    //Docker Image
    //debug true
    queue = "$params.queue"
    container = "quay.io/biocontainers/pybedtools:0.9.0--py38he0f268d_2"
    tag "$result_files"

    publishDir "$params.agg_results", mode : 'copy'

    input:
    path (py_program)
    path (result_files)
    
    output:
    path ('*.csv')

    script:
    """
    python $py_program $result_files
    """
}

process execute_sum{
    //Docker Image
    //debug true
    queue = "$params.queue"
    container = "quay.io/biocontainers/pybedtools:0.9.0--py38he0f268d_2"
    tag "$result_file"

    publishDir "$params.agg_results", mode : 'copy'

    input:
    path (py_program)
    path (result_file)
    
    output:
    path ('*.csv')
    path ('*.bed')

    script:
    """
    python $py_program $result_file
    """
}

workflow{

    //python programs
    //***************
    ch_py_program_count = Channel.fromPath("count_hits.py")
    ch_py_program_merge = Channel.fromPath("merge_df.py")
    ch_py_program_sum = Channel.fromPath("sum_df_m_out.py")


    //bed files
    //***************
    ch_ref_file = Channel.fromPath("$params.ref_file")
    ch_encode_files = Channel.fromPath("$params.encode")

    //create output directories
    ch_dir = create_results_dir()

    //execute count hits for every combination of ref file and encode file
    chSaidaResults = executa_count_hits(ch_py_program_count,ch_ref_file,ch_encode_files,ch_dir)
  
    //Collect all files output and the pass to me program that will merge then
    chAllFiles = chSaidaResults.collectFile().toList()
    chMergedFile = executa_merge(ch_py_program_merge,chAllFiles)

    //create the summed and the bed file
    chSummedFiles = execute_sum(ch_py_program_sum,chMergedFile)
}