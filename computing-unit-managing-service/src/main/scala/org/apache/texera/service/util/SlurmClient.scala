/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.service.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.common.config.{EnvironmentalVariable, SlurmConfig}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try
import scala.util.matching.Regex

/**
  * The result of running an external command.
  */
final case class CommandResult(exitCode: Int, stdout: String, stderr: String) {
  def succeeded: Boolean = exitCode == 0
}

/**
  * Runs an external command. An interface rather than a direct call to `Process` so that the
  * Slurm client can be tested without a scheduler.
  */
trait CommandRunner {
  def run(command: Seq[String]): CommandResult
}

object ProcessCommandRunner extends CommandRunner {
  override def run(command: Seq[String]): CommandResult = {
    val out = new StringBuilder
    val err = new StringBuilder
    val exitCode = Process(command) ! ProcessLogger(
      line => out.append(line).append("\n"),
      line => err.append(line).append("\n")
    )
    CommandResult(exitCode, out.toString.trim, err.toString.trim)
  }
}

/**
  * One row of `squeue` output.
  */
final case class SlurmJobInfo(jobId: String, name: String, state: String, nodes: String)

/**
  * Submits, inspects and cancels the Slurm jobs that back computing units.
  *
  * A unit's address is not known at submission time -- the batch script records `<node>:<port>`
  * into `<state-dir>/<cuid>/uri` once it starts, and the managing service reads it back from
  * there.
  */
class SlurmClient(runner: CommandRunner, stateDirPath: Path) extends LazyLogging {

  def jobName(cuid: Int): String = s"${SlurmClient.jobNamePrefix}$cuid"

  def unitDir(cuid: Int): Path = stateDirPath.resolve(Integer.toString(cuid))

  /**
    * Write a batch script for this unit and submit it. Returns the Slurm job id.
    */
  def submitJob(
      cuid: Int,
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String,
      env: Map[String, Any]
  ): String = {
    val dir = unitDir(cuid)
    Files.createDirectories(dir)

    val script = SlurmClient.buildBatchScript(
      cuid,
      cpuLimit,
      memoryLimit,
      gpuLimit,
      SlurmClient.rewriteEnvForComputeNode(env, SlurmConfig.advertiseHost) ++
        SlurmClient
          .jvmHeapFor(memoryLimit, SlurmConfig.jvmHeapFraction)
          .map(EnvironmentalVariable.ENV_JAVA_OPTS -> _),
      SlurmConfig.partition,
      SlurmConfig.account,
      SlurmConfig.timeLimit,
      SlurmConfig.gpuResourceName,
      dir,
      SlurmConfig.texeraHome,
      SlurmConfig.launcher,
      SlurmConfig.pathPrefix
    )

    val scriptPath = dir.resolve("job.sh")
    Files.write(scriptPath, script.getBytes(StandardCharsets.UTF_8))
    scriptPath.toFile.setExecutable(true, true)

    // A stale uri from a previous incarnation of the same cuid would be read as this one's
    // address, so it goes before the job that writes the new one.
    Files.deleteIfExists(dir.resolve("uri"))

    val result = runner.run(Seq(SlurmConfig.sbatchPath, "--parsable", scriptPath.toString))
    if (!result.succeeded) {
      throw SlurmSubmissionFailed(cuid, SlurmClient.firstNonBlank(result.stderr, result.stdout))
    }

    // --parsable prints "<jobid>" or "<jobid>;<cluster>"; a site's sbatch banner may precede it.
    val jobId = result.stdout.linesIterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList
      .lastOption
      .map(_.split(';').head)
      .getOrElse("")

    if (jobId.isEmpty) {
      throw SlurmSubmissionFailed(cuid, s"sbatch produced no job id (output: ${result.stdout})")
    }

    logger.info("Submitted computing unit {} as Slurm job {}", cuid, jobId)
    jobId
  }

  /**
    * Every Texera job this user owns, keyed by job name.
    *
    * None means "squeue did not answer": the caller must not conclude that a unit is gone,
    * because a scheduler hiccup would then terminate every running unit.
    */
  def getAllJobStates(): Option[Map[String, SlurmJobInfo]] = {
    val result = runner.run(Seq(SlurmConfig.squeuePath, "--me", "-h", "-o", SlurmClient.squeueFormat))
    if (!result.succeeded) {
      logger.warn(
        s"squeue failed (exit ${result.exitCode}): ${result.stderr}. " +
          "Slurm computing-unit state is unknown for this poll; leaving units untouched."
      )
      None
    } else {
      Some(SlurmClient.parseSqueueOutput(result.stdout).filter {
        case (name, _) => name.startsWith(SlurmClient.jobNamePrefix)
      })
    }
  }

  def getJobState(cuid: Int): Option[SlurmJobInfo] = {
    val result = runner.run(
      Seq(SlurmConfig.squeuePath, "--me", "-h", "-n", jobName(cuid), "-o", SlurmClient.squeueFormat)
    )
    if (!result.succeeded) None
    else SlurmClient.parseSqueueOutput(result.stdout).get(jobName(cuid))
  }

  def cancelJob(cuid: Int): Unit = {
    val result = runner.run(Seq(SlurmConfig.scancelPath, "--me", "-n", jobName(cuid)))
    if (!result.succeeded) {
      logger.warn(
        "scancel for computing unit {} returned {}: {}",
        cuid,
        result.exitCode,
        result.stderr
      )
    } else {
      logger.info("Cancelled Slurm job for computing unit {}", cuid)
    }
  }

  /**
    * The `<node>:<port>` the batch script published, if it has started.
    */
  def readRecordedUri(cuid: Int): Option[String] =
    Try(Files.readString(unitDir(cuid).resolve("uri"))).toOption
      .map(_.trim)
      .filter(_.nonEmpty)
}

object SlurmClient
    extends SlurmClient(ProcessCommandRunner, Paths.get(SlurmConfig.stateDir)) {

  val jobNamePrefix: String = "texera-cu-"

  // job id | job name | state | node list
  val squeueFormat: String = "%i|%j|%T|%N"

  val runningStates: Set[String] = Set("RUNNING", "COMPLETING")

  val pendingStates: Set[String] =
    Set("PENDING", "CONFIGURING", "RESIZING", "REQUEUED", "SUSPENDED")

  def firstNonBlank(candidates: String*): String =
    candidates.find(_.trim.nonEmpty).map(_.trim).getOrElse("no output from the command")

  // Matches a loopback host at the start of a value, after "//" (a URL) or after "@" (a JDBC
  // or S3 credential), and only when what follows is a port, a path or the end of the value.
  private val hostToRewrite: Regex = "(^|//|@)(localhost|127\\.0\\.0\\.1)(?=[:/]|$)".r

  /**
    * The environment a computing unit inherits, made usable from a compute node: every
    * loopback endpoint becomes the head node's advertised address, and the ambient toolchain
    * variables the job needs are added from the managing service's own environment.
    */
  def rewriteEnvForComputeNode(env: Map[String, Any], advertiseHost: String): Map[String, String] = {
    val rewritten = env.map {
      case (key, value) =>
        val asString = if (value == null) "" else value.toString
        key -> hostToRewrite.replaceAllIn(asString, matched => matched.group(1) + advertiseHost)
    }
    val passthrough = SlurmConfig.envPassthrough
      .flatMap(name => sys.env.get(name).filter(_.nonEmpty).map(name -> _))
      .toMap
    // The explicit environment wins over the passthrough, never the other way round.
    passthrough ++ rewritten
  }

  /**
    * A Kubernetes-style memory quantity as a whole number of MiB.
    */
  def toMebibytes(quantity: String): Option[Long] = {
    val trimmed = Option(quantity).map(_.trim).getOrElse("")
    val digits = trimmed.takeWhile(_.isDigit)
    if (digits.isEmpty) None
    else {
      val unit = trimmed.drop(digits.length).toUpperCase.stripSuffix("I")
      val amount = digits.toLong
      val mebibytes = unit match {
        case "" | "M" => Some(amount)
        case "K"      => Some(amount / 1024L)
        case "G"      => Some(amount * 1024L)
        case "T"      => Some(amount * 1024L * 1024L)
        case _        => None
      }
      mebibytes.filter(_ > 0L)
    }
  }

  /**
    * -Xmx for a unit of this size, or None to leave the JVM's own default alone.
    */
  def jvmHeapFor(memoryLimit: String, fraction: Double): Option[String] = {
    if (fraction <= 0 || fraction >= 1) None
    else
      toMebibytes(memoryLimit)
        .map(mib => (mib * fraction).toLong)
        .filter(_ >= 256L)
        .map(heapMib => s"-Xmx${heapMib}m")
  }

  /**
    * Kubernetes writes "8Gi"; sbatch --mem wants "8G".
    */
  def toSlurmMemory(memoryLimit: String): String = {
    val trimmed = memoryLimit.trim
    if (trimmed.isEmpty) ""
    else if (
      trimmed.length > 1 && trimmed.endsWith("i") && "KMGTP".contains(trimmed.charAt(trimmed.length - 2))
    ) trimmed.dropRight(1)
    else trimmed
  }

  def parseSqueueOutput(output: String): Map[String, SlurmJobInfo] = {
    if (output == null) Map.empty
    else
      output.linesIterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .flatMap { line =>
          line.split('|') match {
            case Array(jobId, name, state, rest @ _*) =>
              Some(
                name -> SlurmJobInfo(
                  jobId.trim,
                  name.trim,
                  state.trim,
                  rest.headOption.getOrElse("").trim
                )
              )
            case _ => None
          }
        }
        .toMap
  }

  def shellQuote(value: String): String = "'" + value.replace("'", "'\\''") + "'"

  /**
    * The sbatch script for one computing unit.
    */
  def buildBatchScript(
      cuid: Int,
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String,
      env: Map[String, String],
      partition: String,
      account: Option[String],
      timeLimit: String,
      gpuResourceName: String,
      unitDir: Path,
      texeraHome: String,
      launcher: String,
      pathPrefix: Option[String]
  ): String = {
    val directives = List.newBuilder[String]
    directives += s"#SBATCH --job-name=$jobNamePrefix$cuid"
    directives += s"#SBATCH --partition=$partition"
    account.foreach(a => directives += s"#SBATCH --account=$a")
    directives += "#SBATCH --nodes=1"
    directives += "#SBATCH --ntasks=1"
    directives += s"#SBATCH --cpus-per-task=$cpuLimit"
    val mem = toSlurmMemory(memoryLimit)
    if (mem.nonEmpty) directives += s"#SBATCH --mem=$mem"
    directives += s"#SBATCH --time=$timeLimit"
    // Requesting a GRES no node registers leaves the job pending forever, so "0" asks for none.
    if (gpuLimit != null && gpuLimit.trim.nonEmpty && gpuLimit.trim != "0") {
      directives += s"#SBATCH --gres=$gpuResourceName:${gpuLimit.trim}"
    }
    directives += s"#SBATCH --output=${unitDir.resolve("slurm-%j.out")}"
    directives += "#SBATCH --open-mode=append"

    val exports = env.toSeq.sortBy(_._1).map {
      case (key, value) => s"export $key=${shellQuote(value)}"
    }
    val pathExport =
      pathPrefix.map(prefix => s"export PATH=${shellQuote(prefix)}:\"$$PATH\"").toSeq

    s"""#!/bin/bash
       |${directives.result().mkString("\n")}
       |
       |# Texera computing unit $cuid.
       |#
       |# Generated by SlurmClient at submission time; regenerated on every submission, so
       |# edits here do not survive. Runs the same `computing-unit-master` build the head
       |# node runs, reached over the shared filesystem.
       |
       |set -uo pipefail
       |
       |STATE_DIR=${shellQuote(unitDir.toString)}
       |printf '%s' "$$SLURM_JOB_ID" > "$$STATE_DIR/jobid"
       |
       |# Endpoints and credentials inherited from the managing service, with localhost
       |# rewritten to an address that reaches the head node from here.
       |${exports.mkString("\n")}
       |${pathExport.mkString("\n")}
       |
       |# A node can host several computing units, so neither the HTTP port nor the Pekko
       |# port can be fixed. Ask the kernel for free ones and pass them through the
       |# environment, which is why both are configurable rather than hardcoded.
       |read -r CU_PORT PEKKO_PORT < <(python3 -c '
       |import socket
       |
       |
       |def free_port():
       |    sock = socket.socket()
       |    sock.bind(("", 0))
       |    port = sock.getsockname()[1]
       |    sock.close()
       |    return port
       |
       |
       |first = free_port()
       |second = free_port()
       |while second == first:
       |    second = free_port()
       |print(first, second)
       |')
       |
       |if [ -z "$${CU_PORT:-}" ] || [ -z "$${PEKKO_PORT:-}" ]; then
       |  echo "computing unit $cuid: failed to allocate ports" >&2
       |  exit 1
       |fi
       |
       |export COMPUTING_UNIT_MASTER_PORT="$$CU_PORT"
       |export AMBER_PEKKO_PORT="$$PEKKO_PORT"
       |
       |# Publish where this unit can be reached. The managing service polls for this file
       |# and copies it into workflow_computing_unit.uri, which is what the gateway routes
       |# on; until it exists the unit is correctly reported as Pending. Written via a
       |# temporary file so a reader never sees a partial address. SLURMD_NODENAME is used
       |# rather than `hostname -f`, whose fully-qualified form does not necessarily resolve
       |# from the head node.
       |CU_HOST="$${SLURMD_NODENAME:-$$(hostname)}"
       |printf '%s' "$$CU_HOST:$$CU_PORT" > "$$STATE_DIR/uri.tmp"
       |mv "$$STATE_DIR/uri.tmp" "$$STATE_DIR/uri"
       |
       |# ComputingUnitMaster locates its config file relative to the working directory
       |# (Utils.amberHomePath resolves "." then looks for an "amber" directory), so this cd
       |# is required, not cosmetic.
       |cd ${shellQuote(texeraHome)} || exit 1
       |
       |echo "computing unit $cuid starting on $$CU_HOST: http port $$CU_PORT, pekko port $$PEKKO_PORT"
       |exec ${shellQuote(launcher)}
       |""".stripMargin
  }
}
