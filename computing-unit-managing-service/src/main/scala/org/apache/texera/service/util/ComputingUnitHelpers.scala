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

import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import org.apache.texera.service.resource.ComputingUnitManagingResource.{
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitMetrics
}
import org.apache.texera.service.resource.ComputingUnitState.{ComputingUnitState, Pending, Running}
import org.jooq.EnumType
import play.api.libs.json.{JsObject, Json}

import java.sql.Timestamp
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}
import scala.util.Try

object ComputingUnitHelpers {

  /**
    * Owner (avatar, name) keyed by uid, resolved in one query. Blank values collapse to `null`;
    * empty `uids` returns empty without querying.
    */
  def resolveOwnerInfo(
      userDao: UserDao,
      uids: Seq[Integer]
  ): Map[Integer, (String, String)] = {
    if (uids.isEmpty) Map.empty
    else
      userDao
        .fetchByUid(uids: _*)
        .asScala
        .map { u =>
          val avatar = Option(u.getAvatar).filter(_.nonEmpty).orNull
          val name = Option(u.getName).filter(_.nonEmpty).orNull
          u.getUid -> (avatar, name)
        }
        .toMap
  }

  def getComputingUnitStatus(unit: WorkflowComputingUnit): ComputingUnitState =
    singleUnitStatus(unit, KubernetesClient, SlurmClient)

  /**
    * A Slurm unit is Running only once both halves are true: the job is on a node, and the job
    * has published the address the gateway will route to. Between the two it is Pending, which
    * is what the UI needs in order to wait rather than to fail.
    */
  private[util] def slurmUnitStatus(
      unit: WorkflowComputingUnit,
      jobs: Map[String, SlurmJobInfo],
      jobNameOf: Int => String
  ): ComputingUnitState = {
    val jobRunning =
      jobs.get(jobNameOf(unit.getCuid)).exists(info => SlurmClient.runningStates.contains(info.state.toUpperCase))
    val addressKnown = Option(unit.getUri).exists(_.trim.nonEmpty)
    if (jobRunning && addressKnown) Running else Pending
  }

  /**
    * Single-unit status via a per-unit lookup (a targeted GET, cheaper than listing the whole
    * namespace). The clients are by-name parameters — not the global singletons — so each branch
    * is unit-testable with a stub and the local/unknown branches never force a client; the public
    * overload binds the production [[KubernetesClient]] and [[SlurmClient]]. (Metrics has no
    * analogous seam: its per-unit lookup already fans out to the whole namespace and the bulk
    * (unit, podMetrics) overload already covers the cpu/memory resolution.)
    */
  private[util] def singleUnitStatus(
      unit: WorkflowComputingUnit,
      k8s: => KubernetesClient,
      slurm: => SlurmClient = SlurmClient
  ): ComputingUnitState = {
    unit.getType match {
      // Local CUs are always “running”
      case WorkflowComputingUnitTypeEnum.local =>
        Running

      // Slurm CUs – the job must be on a node and have published its address
      case WorkflowComputingUnitTypeEnum.slurm =>
        val client = slurm
        val jobs = client.getJobState(unit.getCuid).map(info => info.name -> info).toMap
        slurmUnitStatus(unit, jobs, cuid => client.jobName(cuid))

      // Kubernetes CUs – only explicit “Running” counts as running
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        // Guard the pod status the same way the bulk getAllPodPhases does: a pod with no
        // status yet has a null getStatus, so map through Option to avoid an NPE.
        val client = k8s
        val phaseOpt = client
          .getPodByName(client.generatePodName(unit.getCuid))
          .flatMap(pod => Option(pod.getStatus).map(_.getPhase))

        if (phaseOpt.contains("Running")) Running else Pending

      // Any other (unknown) type is treated as pending
      case _ =>
        Pending
    }
  }

  def getComputingUnitMetrics(unit: WorkflowComputingUnit): WorkflowComputingUnitMetrics = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val metrics = KubernetesClient.getPodMetrics(unit.getCuid)
        WorkflowComputingUnitMetrics(
          metrics.getOrElse("cpu", ""),
          metrics.getOrElse("memory", "")
        )
      // Slurm reports usage through sacct/sstat, which is a different shape and a different
      // round trip; not offered rather than guessed.
      case WorkflowComputingUnitTypeEnum.slurm =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case _ =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
    }
  }

  /**
    * Resolves status from pre-fetched maps instead of a per-unit cluster call, so a listing costs
    * O(1) round trips rather than one per unit.
    */
  def getComputingUnitStatus(
      unit: WorkflowComputingUnit,
      podPhases: Map[String, String],
      slurmJobs: Map[String, SlurmJobInfo] = Map.empty
  ): ComputingUnitState = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        Running
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        // A missing entry or null phase both count as not-Running.
        if (podPhases.get(KubernetesClient.generatePodName(unit.getCuid)).contains("Running"))
          Running
        else Pending
      case WorkflowComputingUnitTypeEnum.slurm =>
        slurmUnitStatus(unit, slurmJobs, cuid => SlurmClient.jobName(cuid))
      case _ =>
        Pending
    }
  }

  /** Resolves metrics from a pre-fetched pod-metrics map instead of a per-unit cluster call. */
  def getComputingUnitMetrics(
      unit: WorkflowComputingUnit,
      podMetrics: Map[String, Map[String, String]]
  ): WorkflowComputingUnitMetrics = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val metrics = podMetrics
          .getOrElse(KubernetesClient.generatePodName(unit.getCuid), Map.empty[String, String])
        WorkflowComputingUnitMetrics(
          metrics.getOrElse("cpu", ""),
          metrics.getOrElse("memory", "")
        )
      case WorkflowComputingUnitTypeEnum.slurm =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case _ =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
    }
  }

  private def isKubernetes(unit: WorkflowComputingUnit): Boolean =
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.kubernetes => true
      case _                                        => false
    }

  private def isSlurm(unit: WorkflowComputingUnit): Boolean =
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.slurm => true
      case _                                   => false
    }

  // Pod phases/metrics for the namespace; skipped (empty) when no Kubernetes unit is present, so a
  // cluster-free listing issues no round trip. Same seam as singleUnitStatus: the public overload
  // binds the production singleton, passing it by-name to the private[util] overload, which forces
  // it only inside the guard's true branch — so the empty path never touches the client, and tests
  // drive the private overload with a stub.
  def podPhasesFor(units: Seq[WorkflowComputingUnit]): Map[String, String] =
    podPhasesFor(units, KubernetesClient)

  private[util] def podPhasesFor(
      units: Seq[WorkflowComputingUnit],
      k8s: => KubernetesClient
  ): Map[String, String] =
    if (units.exists(isKubernetes)) k8s.getAllPodPhases else Map.empty

  def podMetricsFor(units: Seq[WorkflowComputingUnit]): Map[String, Map[String, String]] =
    podMetricsFor(units, KubernetesClient)

  private[util] def podMetricsFor(
      units: Seq[WorkflowComputingUnit],
      k8s: => KubernetesClient
  ): Map[String, Map[String, String]] =
    if (units.exists(isKubernetes)) k8s.getAllPodMetrics else Map.empty

  /**
    * Slurm job states for the namespace's worth of units, keyed by job name.
    *
    * `None` means squeue did not answer. That is deliberately different from "no jobs": a
    * scheduler that is briefly unreachable must not be read as every unit having disappeared.
    */
  def slurmJobsFor(units: Seq[WorkflowComputingUnit]): Option[Map[String, SlurmJobInfo]] =
    slurmJobsFor(units, SlurmClient)

  private[util] def slurmJobsFor(
      units: Seq[WorkflowComputingUnit],
      slurm: => SlurmClient
  ): Option[Map[String, SlurmJobInfo]] =
    if (units.exists(isSlurm)) slurm.getAllJobStates() else Some(Map.empty)

  /** A Kubernetes unit whose pod is absent from `podPhases` (deleted or TTL GC-ed). */
  private def isVanished(unit: WorkflowComputingUnit, podPhases: Map[String, String]): Boolean =
    isKubernetes(unit) && !podPhases.contains(KubernetesClient.generatePodName(unit.getCuid))

  /**
    * How long a freshly created Slurm unit may be absent from squeue before it counts as gone.
    *
    * sbatch returns before the job is queryable, so a unit created moments ago is legitimately
    * missing from squeue output. Without this window a listing that happened to land in that gap
    * would terminate the unit it had just created.
    */
  private[util] val slurmVanishGracePeriodMs: Long = 60000L

  private[util] def isBeyondSlurmGracePeriod(unit: WorkflowComputingUnit, now: Long): Boolean =
    Option(unit.getCreationTime).forall(created => now - created.getTime > slurmVanishGracePeriodMs)

  /** A Slurm unit with no job in `slurmJobs`, past the grace period. */
  private[util] def isSlurmVanished(
      unit: WorkflowComputingUnit,
      slurmJobs: Map[String, SlurmJobInfo],
      now: Long
  ): Boolean =
    isSlurm(unit) &&
      !slurmJobs.contains(SlurmClient.jobName(unit.getCuid)) &&
      isBeyondSlurmGracePeriod(unit, now)

  /**
    * Partition into `(live, vanished)`. Pure (no I/O), so it is unit-testable.
    *
    * `slurmJobs` is an Option because "squeue did not answer" must leave Slurm units alone; only
    * a definite answer that omits a job is evidence that the job is gone.
    */
  def partitionLiveUnits(
      units: List[WorkflowComputingUnit],
      podPhases: Map[String, String],
      slurmJobs: Option[Map[String, SlurmJobInfo]] = None,
      now: Long = System.currentTimeMillis()
  ): (List[WorkflowComputingUnit], List[WorkflowComputingUnit]) =
    units.partition(unit =>
      !isVanished(unit, podPhases) && !slurmJobs.exists(jobs => isSlurmVanished(unit, jobs, now))
    )

  /**
    * Stamp `terminateTime` on vanished units (one batched update) and return the live ones.
    * Shared by both listing endpoints so they agree on when a unit is terminated.
    */
  def reconcileVanishedUnits(
      dao: WorkflowComputingUnitDao,
      units: List[WorkflowComputingUnit],
      podPhases: Map[String, String],
      slurmJobs: Option[Map[String, SlurmJobInfo]] = None
  ): List[WorkflowComputingUnit] = {
    val partitioned = partitionLiveUnits(units, podPhases, slurmJobs)
    val vanished = partitioned._2
    if (vanished.nonEmpty) {
      val now = new Timestamp(System.currentTimeMillis())
      vanished.foreach(_.setTerminateTime(now))
      dao.update(vanished.asJava)
    }
    partitioned._1
  }

  /**
    * Copy the address a started Slurm job published into the units that do not have one yet.
    *
    * This is where a Slurm unit stops being Pending: the batch script writes `<node>:<port>` to
    * the shared filesystem, and the next listing picks it up.
    */
  def recordSlurmAddresses(
      dao: WorkflowComputingUnitDao,
      units: List[WorkflowComputingUnit],
      slurmJobs: Map[String, SlurmJobInfo]
  ): List[WorkflowComputingUnit] = recordSlurmAddresses(dao, units, slurmJobs, SlurmClient)

  private[util] def recordSlurmAddresses(
      dao: WorkflowComputingUnitDao,
      units: List[WorkflowComputingUnit],
      slurmJobs: Map[String, SlurmJobInfo],
      slurm: => SlurmClient
  ): List[WorkflowComputingUnit] = {
    val awaitingAddress = units.filter(unit =>
      isSlurm(unit) &&
        Option(unit.getUri).forall(_.trim.isEmpty) &&
        slurmJobs
          .get(SlurmClient.jobName(unit.getCuid))
          .exists(info => SlurmClient.runningStates.contains(info.state.toUpperCase))
    )
    if (awaitingAddress.isEmpty) units
    else {
      val client = slurm
      val updated = awaitingAddress.flatMap { unit =>
        client.readRecordedUri(unit.getCuid).map { uri =>
          unit.setUri(uri)
          unit.setResource(withNodeAddress(unit.getResource, uri))
          unit
        }
      }
      if (updated.nonEmpty) dao.update(updated.asJava)
      units
    }
  }

  /** The single-unit form, for the endpoints that fetch one row. */
  def recordSlurmAddress(dao: WorkflowComputingUnitDao, unit: WorkflowComputingUnit): Unit =
    recordSlurmAddress(dao, unit, SlurmClient)

  private[util] def recordSlurmAddress(
      dao: WorkflowComputingUnitDao,
      unit: WorkflowComputingUnit,
      slurm: => SlurmClient
  ): Unit = {
    if (isSlurm(unit) && Option(unit.getUri).forall(_.trim.isEmpty)) {
      val client = slurm
      val jobs = client.getJobState(unit.getCuid).map(info => info.name -> info).toMap
      recordSlurmAddresses(dao, List(unit), jobs, client)
    }
  }

  /**
    * Put `address` into the unit's resource JSON under nodeAddresses, the same field the
    * Kubernetes path fills in, so the dashboard renders both kinds of unit identically.
    */
  private[util] def withNodeAddress(resource: String, address: String): String = {
    val base = Option(resource)
      .filter(_.nonEmpty)
      .flatMap(raw => Try(Json.parse(raw).as[JsObject]).toOption)
      .getOrElse(Json.obj())
    Json.stringify(base ++ Json.obj("nodeAddresses" -> Json.arr(address)))
  }

  /**
    * Build one dashboard row; status/metrics come from the pre-fetched maps (no per-unit cluster
    * call). Shared by both listing endpoints so row shape and owner-info fallback stay identical.
    */
  def buildDashboardUnit(
      unit: WorkflowComputingUnit,
      isOwner: Boolean,
      accessPrivilege: EnumType,
      ownerInfo: Map[Integer, (String, String)],
      podPhases: Map[String, String],
      podMetrics: Map[String, Map[String, String]],
      slurmJobs: Map[String, SlurmJobInfo] = Map.empty
  ): DashboardWorkflowComputingUnit = {
    val owner = ownerInfo.getOrElse(unit.getUid, (null, null))
    DashboardWorkflowComputingUnit(
      computingUnit = unit,
      status = getComputingUnitStatus(unit, podPhases, slurmJobs).toString,
      metrics = getComputingUnitMetrics(unit, podMetrics),
      isOwner = isOwner,
      accessPrivilege = accessPrivilege,
      ownerAvatar = owner._1,
      ownerName = owner._2
    )
  }
}
