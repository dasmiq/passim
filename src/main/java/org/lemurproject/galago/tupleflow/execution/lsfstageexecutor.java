//BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

// Add in galago 3.5.2 and higher
import org.lemurproject.galago.tupleflow.GalagoConf;

/**
 * DRMAAExecutor
 *
 * March 30, 2007 -- Trevor Strohman
 * March 31, 2010 -- Sam Huston, Stanford Chiu
 * Sept 16,  2010 -- irmarc
 *    - Added code for getting timing results
 *
 * Usage : StageExecutionFactory
 *  - when a user submits a job with mode="drmaa"
 *
 * - Keeps track of all stage instances submitted to the drmaa api
 * - Provides some statistics on the running stages
 *
 * @author trevor, sjh, schiu, irmarc
 */
/* Classname has to be all lower case due to StageExecutorFactory */
public class lsfstageexecutor extends CheckpointedStageExecutor {

  // Flag to set the verbose mode (either on or off)
  public boolean verbose;
  // For use with user-defined native specifications
  public String nativeSpecification_each;
  public String nativeSpecification_combined;
  // Use the 'java' specified in the env. variable JAVA_HOME --
  // we don't know what version of java the user called us with :(
  public String command = System.getenv("JAVA_HOME") + File.separator
          + "bin/java";
  // We do know what class path the user envoked us with, so use that
  // when submitting each of the jobs to the cluster.
  public String classPath = System.getProperty("java.class.path");
  // This is the TupleFlow executor we'll be using for each of the jobs.
  public String className = org.lemurproject.galago.tupleflow.execution.LocalStageExecutor.class.getCanonicalName();
  // Arbitrary starting and max heap sizes.
  public static final String MEMORY_X = "-Xmx1700m";
  public static final String MEMORY_S = "-Xms1700m";
  public static final String DEFAULT_ENCODING = System.getProperty("file.encoding", "UTF-8");
  // This holds the location that should be used to write temporary files
  // on the nodes.
  public static final String NODE_TEMP_DIR =
          System.getProperty("java.io.tmpdir");
  // These will hold the starting and max heaps passed in by the executor
  // calling us, if any.
  public String memory_x;
  public String memory_s;
  public String nodeTempDir;

  public class DRMAAResult implements StageExecutionStatus {

    ArrayList<String> jobs;
    HashMap<String, File> jobCheckpoints = null;
    HashMap<String, Long> startTimes = null;
    HashMap<String, Long> stopTimes = null;
    ArrayList<Exception> exceptions;
    String stageName;

    public DRMAAResult(String n, ArrayList<String> jobs, HashMap<String, File> jobCheckpoints, HashMap<String, Long> starts) {
      this(n, jobs, jobCheckpoints, starts, null);
    }

    public DRMAAResult(String n, ArrayList<String> jobs, HashMap<String, File> jobCheckpoints, HashMap<String, Long> starts, Exception e) {
      this.jobs = jobs;
      this.jobCheckpoints = jobCheckpoints;
      this.startTimes = starts;
      this.stopTimes = new HashMap<String, Long>();
      this.stageName = n;
      this.exceptions = new ArrayList<Exception>();
      if (e != null) {
        this.exceptions.add(e);
      }
    }

    @Override
    public ArrayList<Exception> getExceptions() {
      return exceptions;
    }

    @Override
    public String getName() {
      return stageName;
    }

    @Override
    public int getCompletedInstances() {
	return 0;
    }

    @Override
    public int getRunningInstances() {
	return 0;
    }

    @Override
    public int getQueuedInstances() {
        return 0;
    }

    @Override
    public int getBlockedInstances() {
	return 0;
    }

    @Override
    public synchronized List<Double> getRunTimes() {
      ArrayList<Double> times = new ArrayList();
      return times;
    }

    @Override
    public boolean isDone() {
      return true;
    }
  }

  /**
   * <p>Creates a new instance of LSFExecutor.</p>
   *
   * @param arguments     An array; if it contains anything, the first
   *                      element is used as the command when submitting
   *                      jobs to DRMAA.
   */
    //public lsfstageexecutor(String[] args) {
  public lsfstageexecutor() {

    // Set the defaults for each job.
    setMemoryUsage(MEMORY_X, MEMORY_S);
    nodeTempDir = NODE_TEMP_DIR;
    verbose = true;
    nativeSpecification_each = "";
    nativeSpecification_combined = "";
    // CIIR specific parameters
    // First get the hostname
    String hostname;
    try {
      InetAddress local = InetAddress.getLocalHost();
      hostname = local.getHostName();
    } catch (UnknownHostException ex) {
      hostname = "localhost";
    }

    // Should change in galago 3.5.2 and higher
    Parameters defaults = GalagoConf.getDrmaaOptions();
    // Parameters defaults = Utility.getDrmaaOptions();
    if (defaults.containsKey("mem")) {
      String mem = defaults.getString("mem");
      assert (!mem.startsWith("-X")) : "Error: mem parameter in .galago.conf file should not start with '-Xmx' or '-Xms'.";
      setMemoryUsage("-Xmx" + defaults.getString("mem"), "-Xms" + defaults.getString("mem"));
    }
    if (defaults.containsKey("nativeSpec")) {
      setNativeSpecification(defaults.getString("nativeSpec"));
    }
    if (defaults.containsKey("nativeSpecEach")) {
      nativeSpecification_each = nativeSpecification_each + " " + defaults.getString("nativeSpecEach");
    }
    if (defaults.containsKey("nativeSpecCombined")) {
      nativeSpecification_combined = nativeSpecification_combined + " " + defaults.getString("nativeSpecCombined");
    }

    // customize based upon arguments

    // for (int i = 0; i < args.length; i++) {
    //   if (args[i].startsWith("Xmx")) {
    //     memory_x = "-" + args[i];
    //   } else if (args[i].startsWith("Xms")) {
    //     memory_s = "-" + args[i];
    //   } else if (args[i].startsWith("-t=")) {
    //     nodeTempDir = args[i].replace("-t=", "");
    //   } else if (args[i].startsWith("-v")) {
    //     verbose = true;
    //   } else if (args[i].startsWith("-ns=")) {
    //     String ns = args[i].replaceAll("^-ns=", "");
    //     ns.replaceAll("(^\")|(\"$)", "");
    //     setNativeSpecification(ns);
    //   } else {
    //     System.out.println("Ignoring unknown argument: " + args[i]);
    //   }
    // }
  }

  // dont need these function anymore....
  /**
   * <p>Sets the verbose mode.</p>
   *
   * @param verbose   The verbose mode.
   */
  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  /**
   * <p>Sets a native specification.</p>
   *
   * @param nativeSpecification The specification to set.
   */
  public void setNativeSpecification(String nativeSpecification) {
    this.nativeSpecification_each = nativeSpecification_each + " " + nativeSpecification;
    this.nativeSpecification_combined = nativeSpecification_combined + " " + nativeSpecification;
  }

  /**
   * <p>Sets the initial heap space (<code>memory_s</code>) and the
   * maximum heap space (<code>memory_x</code>). See the values of
   * <code>MEMORY_X</code> and <code>MEMORY_S</code> for the defaults. </p>
   *
   * @param memory_x  The max heap space to use.
   * @param memory_s  The initial heap space to use.
   */
  public void setMemoryUsage(String memory_x, String memory_s) {
    this.memory_x = memory_x;
    this.memory_s = memory_s;
  }

  /**
   * <p>Sets the temporary directory to use for scratch space on each of the
   * nodes.</p>
   *
   * @param nodeTempDir   The path to the temporary directory.
   */
  public void setNodeTempDir(String nodeTempDir) {
    this.nodeTempDir = nodeTempDir;
  }

  @Override
  public void shutdown() {
  }

  /**
   * <p>Submit all of the jobs for the given stage to the cluster.</p>
   *
   * @param stageName     The stage whose jobs are being submitted.
   * @param jobPaths      The path to each of the jobs being submitted.
   * @param temporary     The path to the temporary directory to use. This
   *                      is where the stdout/stderr files will be stored.
   *
   * @return The results of the jobs and eny errors that were thrown.
   */
  @Override
  public StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths,
          String temporary) {
    ArrayList<String> jobs = new ArrayList<String>();
    HashMap<String, File> jobCheckpoints = new HashMap<String, File>();
    HashMap<String, Long> startTimes = new HashMap<String, Long>();
    try {
	// Cycle through each of the jobs for the given stage.
	System.out.print("bsub -J '" + stageName);
	if ( jobPaths.size() > 1 ) {
	    System.out.print("[1-" + jobPaths.size() + "]%100");
	}
	System.out.print("'");
	System.out.print(" -o '" + temporary + File.separator + "stdout"
			 + File.separator + stageName + "-%I'");
	System.out.print(" -e '" + temporary + File.separator + "stderr"
			 + File.separator + stageName + "-%I'");
	System.out.print(" '" + command);
        String[] arguments = new String[]{"-ea", memory_x, memory_s,
					  "-Djava.io.tmpdir=" + nodeTempDir,
					  "-Dfile.encoding=" + DEFAULT_ENCODING,
					  "-cp", classPath, className};
	for (int index = 0; index < arguments.length; index++) {
            System.out.print(" " + arguments[index]);
	}
	String path1 = jobPaths.get(0);
	if ( jobPaths.size() > 1 ) {
	    System.out.print(" " + path1.substring(0, path1.length() - 1) + "$(( $LSB_JOBINDEX - 1 ))");
	} else {
	    System.out.print(" " + path1);
	}
	System.out.print("'\n");
	
        // // Create the fill a DRMAA job template.
        // JobTemplate template = session.createJobTemplate();
        // template.setJobName("galago-" + stageName + "-" + i);
        // template.setRemoteCommand(command);
        // template.setArgs(Arrays.asList(arguments));
        // template.setOutputPath(":" + temporary + File.separator
        //         + "stdout");
        // template.setErrorPath(":" + temporary + File.separator
        //         + "stderr");

        // // If the user wants the jobs submitted to a particular
        // // queue, set that here.
        // if (jobPaths.size() == 1) {
        //   // if there's only one job - long queue
        //   if (nativeSpecification_combined.length() > 0) {
        //     template.setNativeSpecification(nativeSpecification_combined);
        //   }
        // } else {
        //   // otherwise use the short queue
        //   if (nativeSpecification_each.length() > 0) {
        //     template.setNativeSpecification(nativeSpecification_each);
        //   }
        // }

        // // Run the job.
        // String id = session.runJob(template);
        // int status = session.getJobProgramStatus(id);
        // if (status == Session.FAILED) {
        //   System.err.println("ERROR: Job failed! [" + stageName
        //           + "-" + i + "]");
        // }

        // // Keep track of the job id.
        // jobs.add(id);
        // jobCheckpoints.put(id, new File(jobPaths.get(i) + ".complete"));

        // startTimes.put(id, System.currentTimeMillis()); // for tracking

        // // Clean up.
        // session.deleteJobTemplate(template);

    } catch (Exception e) {
	System.err.println("Problems submitting jobs: " + e.getMessage());
	return new DRMAAResult(stageName, jobs, jobCheckpoints, null, e);
    }
    
    return new DRMAAResult(stageName, jobs, jobCheckpoints, startTimes);
  }
}
