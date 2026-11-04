/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.service.util

import org.apache.texera.common.config.ComputingUnitConfig
import org.slf4j.LoggerFactory
import play.api.libs.json._

import java.io.IOException
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import scala.util.Try

/** What slurmrestd reports about a job, reduced to what decides a unit's status. */
final case class SlurmJobSnapshot(
    states: Set[String],
    node: Option[String],
    reason: Option[String]
)

sealed trait SlurmJobLookup
final case class SlurmJobFound(job: SlurmJobSnapshot) extends SlurmJobLookup

/** Slurm says the job does not exist: slurmctld keeps a finished job for MinJobAge, then drops it. */
case object SlurmJobGone extends SlurmJobLookup

/** No answer that says anything about the job, so its unit must not be treated as dead. */
final case class SlurmJobUnavailable(detail: String) extends SlurmJobLookup

/** The unit's token was refused or is gone, so nothing is known about its job until it ends. */
case object SlurmTokenUnusable extends SlurmJobLookup

/**
  * A user's own slurmrestd identity, for a slurmrestd using rest_auth/jwt.
  *
  * Only `parse` can make one, because these go verbatim into the headers of a hand-written HTTP
  * request: a line break in either would let a user add headers or smuggle a request.
  */
final class SlurmCredentials private (val userName: String, val token: String) {
  def headers: Seq[(String, String)] =
    Seq("X-SLURM-USER-NAME" -> userName, "X-SLURM-USER-TOKEN" -> token)

  override def toString: String = s"SlurmCredentials($userName)"
}

object SlurmCredentials {
  private val ValidUserName = "[\\x21-\\x7E]{1,256}".r
  private val ValidToken = "[A-Za-z0-9._-]{1,8192}".r

  /** None when both are blank, meaning the unit authenticates through rest_auth/local. */
  def parse(userName: Option[String], token: Option[String]): Option[SlurmCredentials] = {
    def entered(value: Option[String]) = value.map(_.trim).filter(_.nonEmpty)
    (entered(userName), entered(token)) match {
      case (None, None) => None
      case (Some(ValidUserName()), Some(ValidToken())) =>
        Some(new SlurmCredentials(entered(userName).get, entered(token).get))
      case (Some(_), Some(_)) =>
        throw InvalidSlurmCredentials("The Slurm user name or token has invalid characters.")
      case _ =>
        throw InvalidSlurmCredentials("Provide both a Slurm user name and a token, or neither.")
    }
  }
}

/**
  * The credentials of running units, in memory only. They are not written to the database or the
  * job, so a restart of this service loses them and those units' jobs can no longer be managed.
  */
object SlurmCredentialStore {
  private val byCuid = new ConcurrentHashMap[Integer, SlurmCredentials]()

  def put(cuid: Integer, credentials: SlurmCredentials): Unit = byCuid.put(cuid, credentials)
  def get(cuid: Integer): Option[SlurmCredentials] = Option(byCuid.get(cuid))
  def remove(cuid: Integer): Unit = byCuid.remove(cuid)
}

object SlurmJobRequest {

  /**
    * The administrator's job JSON with this unit's additions. The administrator's environment
    * overrides the unit's, because the manager's addresses are cluster-internal and a Slurm node
    * usually needs others. `identity` says which unit and user this is and always applies.
    *
    * The parse failure is deliberately not repeated: its message quotes the source, and the job
    * JSON can hold credentials.
    */
  def build(
      jobJson: String,
      cuid: Int,
      unitEnv: Map[String, Any],
      identity: Map[String, Any]
  ): JsObject = {
    val template = Try(Json.parse(jobJson)).toOption
      .collect { case obj: JsObject => obj }
      .getOrElse(throw InvalidSlurmJobJson("it is not a JSON object"))
    val job = (template \ "job").asOpt[JsObject].getOrElse {
      throw InvalidSlurmJobJson("it has no \"job\" object")
    }
    val merged =
      unitEnv.map { case (name, value) => name -> value.toString } ++
        administratorEnvironment(job) ++
        identity.map { case (name, value) => name -> value.toString }
    val environment = merged.toSeq.map { case (name, value) => s"$name=$value" }

    val named =
      if (job.keys.contains("name")) job
      else job + ("name" -> JsString(s"texera-computing-unit-$cuid"))
    template + ("job" -> (named + ("environment" -> Json.toJson(environment))))
  }

  private def administratorEnvironment(job: JsObject): Map[String, String] =
    (job \ "environment").toOption.fold(Map.empty[String, String]) { value =>
      value
        .asOpt[Seq[String]]
        .getOrElse(throw InvalidSlurmJobJson("job.environment is not a list of strings"))
        .map { entry =>
          entry.split("=", 2) match {
            case Array(name, assigned) => name -> assigned
            case _ =>
              throw InvalidSlurmJobJson("job.environment has an entry that is not NAME=value")
          }
        }
        .toMap
    }
}

object SlurmUnitAddress {
  def resolve(template: String, node: String): String = template.replace("{node}", node)
}

/**
  * slurmrestd's job endpoints. `send` is a constructor parameter so the spec can answer with the
  * bodies slurmrestd really returned, without a socket.
  *
  * Without credentials, authentication is the socket itself: rest_auth/local reads the peer's
  * credentials off it and requires slurmrestd's own uid, so the job runs as that uid. With them,
  * rest_auth/jwt authenticates the headers and the job runs as their user.
  */
class SlurmRestClient(
    send: (String, String, Option[String], Seq[(String, String)]) => UnixSocketHttpResponse,
    apiVersion: String
) {
  private val logger = LoggerFactory.getLogger(classOf[SlurmRestClient])

  private def jobUrl(jobId: Long) = s"/slurm/$apiVersion/job/$jobId"

  private def exchange(
      method: String,
      path: String,
      body: Option[String],
      credentials: Option[SlurmCredentials]
  ): UnixSocketHttpResponse =
    try {
      send(method, path, body, credentials.fold(Seq.empty[(String, String)])(_.headers))
    } catch {
      // The detail goes to the log only: it names a host path no user needs to see.
      case e: IOException =>
        logger.warn(s"slurmrestd did not answer $method $path", e)
        throw SlurmRequestFailure("Slurm cannot be reached right now.", 503)
    }

  private def failOnRejectedConnection(response: UnixSocketHttpResponse): Unit = {
    if (response.status == 401) {
      logger.error(
        "slurmrestd rejected this service. With rest_auth/local, this process must run as " +
          "slurmrestd's uid (workflowComputingUnitManager.slurm.runAsUser in the chart), and " +
          "slurmrestd must run on the host rather than in a container of its own: from there " +
          "it cannot see this process's pid, and refuses it whatever the uid. With " +
          "rest_auth/jwt, a request without a user name and token is refused."
      )
      throw SlurmRequestFailure(
        "Slurm rejected this deployment's connection. If it uses tokens, provide a user name " +
          "and token.",
        503
      )
    }
    // slurmrestd passes a token through unchecked. It is slurmctld that refuses a bad or expired
    // one, so this arrives as an error on the request rather than as a 401.
    val refused = Try(Json.parse(response.body)).toOption.toSeq
      .flatMap(slurmErrors)
      .exists(_._1 == SlurmErrno.AuthenticationError)
    if (refused) throw SlurmTokenRejected()
  }

  private def slurmErrors(body: JsValue): Seq[(Int, String)] =
    (body \ "errors").asOpt[Seq[JsObject]].getOrElse(Seq.empty).map { error =>
      (
        (error \ "error_number").asOpt[Int].getOrElse(0),
        (error \ "error").asOpt[String].filter(_.nonEmpty).getOrElse("unknown error")
      )
    }

  /** Submits the body as it is and returns the job id. Nothing from the body reaches a message. */
  def submitJob(body: JsObject, credentials: Option[SlurmCredentials] = None): Long = {
    val response =
      exchange("POST", s"/slurm/$apiVersion/job/submit", Some(Json.stringify(body)), credentials)
    failOnRejectedConnection(response)
    val parsed = Try(Json.parse(response.body)).toOption
    val jobId = parsed.flatMap(json => (json \ "job_id").asOpt[Long])
    jobId.filter(_ => response.status == 200).getOrElse {
      val said = parsed.toSeq.flatMap(slurmErrors).map(_._2)
      val detail = if (said.isEmpty) s"HTTP ${response.status}" else said.mkString("; ")
      throw SlurmRequestFailure(s"Slurm did not accept the computing unit's job: $detail", 502)
    }
  }

  /** Never throws: one unit's job being unreadable must not fail a listing of every unit. */
  def lookupJob(jobId: Long, credentials: Option[SlurmCredentials] = None): SlurmJobLookup =
    try {
      interpretLookup(exchange("GET", jobUrl(jobId), None, credentials))
    } catch {
      case e: SlurmRequestFailure => SlurmJobUnavailable(e.getMessage)
      case _: SlurmTokenRejected  => SlurmTokenUnusable
    }

  private def interpretLookup(response: UnixSocketHttpResponse): SlurmJobLookup = {
    failOnRejectedConnection(response)
    Try(Json.parse(response.body)).toOption match {
      case None => SlurmJobUnavailable(s"HTTP ${response.status} with a body that is not JSON")
      // Only this error number means the job is gone. A wrong api-version is a 404 as well, and
      // reading that as gone would retire every unit on the deployment.
      case Some(json) if slurmErrors(json).exists(_._1 == SlurmErrno.InvalidJobId) => SlurmJobGone
      case Some(json) if response.status == 200 =>
        (json \ "jobs" \ 0).asOpt[JsObject].flatMap(parseJob) match {
          case Some(job) => SlurmJobFound(job)
          case None      => SlurmJobUnavailable("the response describes no job")
        }
      case Some(_) => SlurmJobUnavailable(s"HTTP ${response.status}")
    }
  }

  private def parseJob(job: JsObject): Option[SlurmJobSnapshot] = {
    val states = (job \ "job_state") match {
      case JsDefined(JsString(state)) => Set(state)
      case JsDefined(value)           => value.asOpt[Set[String]].getOrElse(Set.empty[String])
      case _                          => Set.empty[String]
    }
    Option.when(states.nonEmpty)(
      SlurmJobSnapshot(
        states,
        (job \ "batch_host").asOpt[String].filter(_.nonEmpty),
        (job \ "state_reason").asOpt[String].filter(reason => reason.nonEmpty && reason != "None")
      )
    )
  }

  /**
    * Succeeds if the job is gone afterwards, including when it already was.
    *
    * The credentials must be those the job was submitted with. On Slurm 24.11.5, cancelling
    * another user's job through slurmrestd leaves slurmctld unresponsive to every user.
    */
  def cancelJob(jobId: Long, credentials: Option[SlurmCredentials] = None): Unit = {
    val response = exchange("DELETE", jobUrl(jobId), None, credentials)
    failOnRejectedConnection(response)
    val parsed = Try(Json.parse(response.body)).toOption
    // Newer API versions answer 200 and list a per-job error under "status"; older ones use the
    // status code and "errors".
    val reported = parsed.toSeq.flatMap(json => slurmErrors(json) ++ perJobErrors(json))
    val failures = reported.filterNot { case (code, _) => SlurmErrno.JobAlreadyOver(code) }
    val answeredForTheJob = parsed.isDefined && (response.status == 200 || reported.nonEmpty)
    if (failures.nonEmpty || !answeredForTheJob) {
      val detail =
        if (failures.isEmpty) s"HTTP ${response.status}" else failures.map(_._2).mkString("; ")
      throw SlurmRequestFailure(s"Slurm did not cancel the computing unit's job: $detail", 502)
    }
  }

  private def perJobErrors(body: JsValue): Seq[(Int, String)] =
    (body \ "status").asOpt[Seq[JsObject]].getOrElse(Seq.empty).flatMap { status =>
      (status \ "error").asOpt[JsObject].map { error =>
        (
          (error \ "code").asOpt[Int].getOrElse(0),
          (error \ "message").asOpt[String].getOrElse("unknown error")
        )
      }
    }
}

private object SlurmErrno {
  val AuthenticationError = 1007
  val InvalidJobId = 2017
  val JobAlreadyCompleting = 2021
  val JobAlreadyOver: Set[Int] = Set(InvalidJobId, JobAlreadyCompleting)
}

/** Production singleton. Connects per call, so a deployment without Slurm never touches a socket. */
object SlurmRestClient
    extends SlurmRestClient(
      new UnixSocketHttpClient(
        Paths.get(ComputingUnitConfig.slurmSocketPath),
        Duration.ofSeconds(ComputingUnitConfig.slurmRequestTimeoutSeconds.toLong)
      ).send,
      ComputingUnitConfig.slurmApiVersion
    )
