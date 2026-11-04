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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{JsObject, JsString, Json}

import java.io.IOException

class SlurmRestClientSpec extends AnyFlatSpec with Matchers {

  private type Request = (String, String, Option[String])

  /** A client answering from a canned response, recording what it was asked. */
  private def clientAnswering(
      answer: => UnixSocketHttpResponse
  ): (SlurmRestClient, () => List[Request]) = {
    var requests = List.empty[Request]
    val client = new SlurmRestClient(
      (method, path, body, _) => {
        requests = requests :+ ((method, path, body))
        answer
      },
      "v0.0.42"
    )
    (client, () => requests)
  }

  private def ok(body: String) = UnixSocketHttpResponse(200, body)

  // Trimmed from what slurmrestd 24.11 returned to these same calls.
  private val invalidJobBody =
    """{"jobs":[],"errors":[{"description":"Unable to query JobId=999999","error_number":2017,
      |"error":"Invalid job id specified","source":"_handle_job_get"}],"warnings":[]}""".stripMargin
  private val runningJobBody =
    """{"jobs":[{"job_id":4,"job_state":["RUNNING"],"batch_host":"node-1","nodes":"node-1",
      |"state_reason":"None","state_description":""}],"errors":[],"warnings":[]}""".stripMargin
  private val submitRefusedBody =
    """{"errors":[{"description":"Batch job submission failed","error_number":2000,
      |"error":"Invalid partition name specified","source":"slurm_submit_batch_job()"}],
      |"warnings":[]}""".stripMargin

  "submitJob" should "POST the body to the configured API version and return the job id" in {
    val (client, requests) = clientAnswering(ok("""{"job_id":12,"errors":[],"warnings":[]}"""))
    val body = Json.obj("job" -> Json.obj("name" -> "x"))

    client.submitJob(body) shouldBe 12L

    requests() shouldBe List(("POST", "/slurm/v0.0.42/job/submit", Some(Json.stringify(body))))
  }

  it should "report what Slurm said when it refuses the job" in {
    val (client, _) = clientAnswering(UnixSocketHttpResponse(500, submitRefusedBody))
    val thrown = the[SlurmRequestFailure] thrownBy client.submitJob(Json.obj())
    thrown.getMessage should include("Invalid partition name specified")
    thrown.getResponse.getStatus shouldBe 502
  }

  it should "fail on a 200 that carries errors and no job id" in {
    val (client, _) = clientAnswering(ok(submitRefusedBody))
    an[SlurmRequestFailure] should be thrownBy client.submitJob(Json.obj())
  }

  it should "fail on a 200 whose body is not JSON" in {
    val (client, _) = clientAnswering(ok("<html>"))
    an[SlurmRequestFailure] should be thrownBy client.submitJob(Json.obj())
  }

  it should "explain a 401 as the uid mismatch it is under rest_auth/local" in {
    val (client, _) = clientAnswering(UnixSocketHttpResponse(401, "Authentication failure"))
    val thrown = the[SlurmRequestFailure] thrownBy client.submitJob(Json.obj())
    thrown.getMessage should include("rejected")
    thrown.getResponse.getStatus shouldBe 503
  }

  it should "answer 503 when the socket cannot be reached" in {
    val (client, _) = clientAnswering(throw new IOException("Connection refused"))
    val thrown = the[SlurmRequestFailure] thrownBy client.submitJob(Json.obj())
    thrown.getResponse.getStatus shouldBe 503
  }

  // The body holds the unit's database password and the user's token.
  it should "never repeat the submitted body or the transport's detail in its message" in {
    val secret = "s3cret-token"
    val (client, _) = clientAnswering(UnixSocketHttpResponse(500, submitRefusedBody))
    val thrown = the[SlurmRequestFailure] thrownBy client.submitJob(
      Json.obj("job" -> Json.obj("environment" -> Json.arr(s"TOKEN=$secret")))
    )
    thrown.getMessage should not include secret

    val (unreachable, _) = clientAnswering(throw new IOException("/var/run/secret/path"))
    val unreachableThrown = the[SlurmRequestFailure] thrownBy unreachable.submitJob(Json.obj())
    unreachableThrown.getMessage should not include "/var/run/secret/path"
  }

  "lookupJob" should "parse a running job" in {
    val (client, requests) = clientAnswering(ok(runningJobBody))
    client.lookupJob(4L) shouldBe SlurmJobFound(
      SlurmJobSnapshot(Set("RUNNING"), Some("node-1"), None)
    )
    requests() shouldBe List(("GET", "/slurm/v0.0.42/job/4", None))
  }

  it should "read every state flag, and treat blank hosts and a None reason as absent" in {
    val (client, _) = clientAnswering(
      ok(
        """{"jobs":[{"job_state":["PENDING","CONFIGURING"],"batch_host":"",
          |"state_reason":"None"}]}""".stripMargin
      )
    )
    client.lookupJob(4L) shouldBe SlurmJobFound(
      SlurmJobSnapshot(Set("PENDING", "CONFIGURING"), None, None)
    )
  }

  it should "carry the reason a job is waiting" in {
    val (client, _) = clientAnswering(
      ok("""{"jobs":[{"job_state":["PENDING"],"state_reason":"Resources"}]}""")
    )
    client.lookupJob(4L) shouldBe SlurmJobFound(
      SlurmJobSnapshot(Set("PENDING"), None, Some("Resources"))
    )
  }

  // API versions before v0.0.40 report the state as a single string.
  it should "accept a state reported as a string" in {
    val (client, _) = clientAnswering(ok("""{"jobs":[{"job_state":"RUNNING"}]}"""))
    client.lookupJob(4L) shouldBe SlurmJobFound(SlurmJobSnapshot(Set("RUNNING"), None, None))
  }

  it should "report a job Slurm says does not exist as gone" in {
    val (client, _) = clientAnswering(UnixSocketHttpResponse(404, invalidJobBody))
    client.lookupJob(999999L) shouldBe SlurmJobGone
  }

  // A wrong api-version answers 404 too. Reading that as "gone" would retire every running unit.
  it should "not mistake a 404 for an unknown URL as a vanished job" in {
    val (client, _) = clientAnswering(
      UnixSocketHttpResponse(404, "Unable to find requested URL")
    )
    client.lookupJob(4L) shouldBe a[SlurmJobUnavailable]
  }

  it should "not mistake a 404 carrying other errors as a vanished job" in {
    val (client, _) = clientAnswering(
      UnixSocketHttpResponse(404, """{"jobs":[],"errors":[{"error_number":1,"error":"x"}]}""")
    )
    client.lookupJob(4L) shouldBe a[SlurmJobUnavailable]
  }

  it should "not read an empty job list as gone" in {
    val (client, _) = clientAnswering(ok("""{"jobs":[],"errors":[]}"""))
    client.lookupJob(4L) shouldBe a[SlurmJobUnavailable]
  }

  it should "report an unreachable socket as unavailable rather than failing the listing" in {
    val (client, _) = clientAnswering(throw new IOException("Connection refused"))
    client.lookupJob(4L) shouldBe a[SlurmJobUnavailable]
  }

  it should "report a server error and a malformed body as unavailable" in {
    val (serverError, _) = clientAnswering(UnixSocketHttpResponse(500, "{}"))
    serverError.lookupJob(4L) shouldBe a[SlurmJobUnavailable]
    val (malformed, _) = clientAnswering(ok("not json"))
    malformed.lookupJob(4L) shouldBe a[SlurmJobUnavailable]
    val (noState, _) = clientAnswering(ok("""{"jobs":[{"job_id":4}]}"""))
    noState.lookupJob(4L) shouldBe a[SlurmJobUnavailable]
  }

  private def cancelResult(code: Int, message: String) =
    ok(s"""{"status":[{"error":{"string":"$message","code":$code,"message":"$message"}}],
         |"errors":[],"warnings":[]}""".stripMargin)

  "cancelJob" should "DELETE the job" in {
    val (client, requests) = clientAnswering(ok("""{"status":[],"errors":[],"warnings":[]}"""))
    client.cancelJob(4L)
    requests() shouldBe List(("DELETE", "/slurm/v0.0.42/job/4", None))
  }

  // Slurm answers 200 and puts these in the per-job status list.
  it should "treat a job that is gone or already over as cancelled" in {
    val (gone, _) = clientAnswering(cancelResult(2017, "Invalid job id specified"))
    noException should be thrownBy gone.cancelJob(4L)
    val (over, _) = clientAnswering(cancelResult(2021, "Job/step already completing or completed"))
    noException should be thrownBy over.cancelJob(4L)
  }

  it should "fail when Slurm refuses, so the unit is not marked terminated with a live job" in {
    val (refused, _) = clientAnswering(cancelResult(1, "Access/permission denied"))
    val thrown = the[SlurmRequestFailure] thrownBy refused.cancelJob(4L)
    thrown.getMessage should include("Access/permission denied")

    val (serverError, _) = clientAnswering(UnixSocketHttpResponse(500, "{}"))
    an[SlurmRequestFailure] should be thrownBy serverError.cancelJob(4L)

    val (unreachable, _) = clientAnswering(throw new IOException("Connection refused"))
    an[SlurmRequestFailure] should be thrownBy unreachable.cancelJob(4L)
  }

  "SlurmJobRequest.build" should "keep what the administrator wrote and add the unit's env" in {
    val template =
      """{"job":{"partition":"gpu","cpus_per_task":4,"environment":["PATH=/usr/bin"],
        |"script":"#!/bin/bash\nrun"},"extra":1}""".stripMargin

    val built = SlurmJobRequest.build(template, 9, Map("A" -> "1", "B" -> 2), Map("ID" -> "9"))

    (built \ "extra").as[Int] shouldBe 1
    val job = (built \ "job").as[JsObject]
    (job \ "partition").as[String] shouldBe "gpu"
    (job \ "cpus_per_task").as[Int] shouldBe 4
    (job \ "script").as[String] shouldBe "#!/bin/bash\nrun"
    (job \ "environment").as[Seq[String]] should contain theSameElementsAs Seq(
      "A=1",
      "B=2",
      "PATH=/usr/bin",
      "ID=9"
    )
  }

  // The manager's addresses are cluster-internal, and do not resolve from a Slurm node.
  it should "let the administrator's environment override the unit's" in {
    val built = SlurmJobRequest.build(
      """{"job":{"environment":["STORAGE_JDBC_URL=jdbc:postgresql://db.hpc:5432/x"]}}""",
      9,
      Map("STORAGE_JDBC_URL" -> "jdbc:postgresql://texera-postgresql:5432/x"),
      Map.empty
    )
    (built \ "job" \ "environment").as[Seq[String]] shouldBe Seq(
      "STORAGE_JDBC_URL=jdbc:postgresql://db.hpc:5432/x"
    )
  }

  // The chart puts the addresses it publishes ahead of the administrator's entries, which relies on
  // this to leave the administrator's in charge.
  it should "keep the last entry for a name the job JSON repeats" in {
    val built = SlurmJobRequest.build(
      """{"job":{"environment":["STORAGE_S3_ENDPOINT=http://published","STORAGE_S3_ENDPOINT=http://own"]}}""",
      9,
      Map.empty,
      Map.empty
    )
    (built \ "job" \ "environment").as[Seq[String]] shouldBe Seq(
      "STORAGE_S3_ENDPOINT=http://own"
    )
  }

  it should "not let the administrator's environment override the unit's identity" in {
    val built = SlurmJobRequest.build(
      """{"job":{"environment":["USER_JWT_TOKEN=shared"]}}""",
      9,
      Map.empty,
      Map("USER_JWT_TOKEN" -> "owner")
    )
    (built \ "job" \ "environment").as[Seq[String]] shouldBe Seq("USER_JWT_TOKEN=owner")
  }

  it should "keep a value holding '=' whole" in {
    val built = SlurmJobRequest.build(
      """{"job":{"environment":["OPTS=-Da=b -Dc=d"]}}""",
      9,
      Map.empty,
      Map.empty
    )
    (built \ "job" \ "environment").as[Seq[String]] shouldBe Seq("OPTS=-Da=b -Dc=d")
  }

  it should "name the job after the unit, unless the administrator named it" in {
    val named = SlurmJobRequest.build("""{"job":{"name":"mine"}}""", 9, Map.empty, Map.empty)
    (named \ "job" \ "name").get shouldBe JsString("mine")
    val unnamed = SlurmJobRequest.build("""{"job":{}}""", 9, Map.empty, Map.empty)
    (unnamed \ "job" \ "name").get shouldBe JsString("texera-computing-unit-9")
  }

  it should "reject JSON an administrator could only have meant differently" in {
    Seq(
      "",
      "   ",
      "not json",
      "[]",
      "{}",
      """{"job":[]}""",
      """{"job":{"environment":"A=b"}}""",
      """{"job":{"environment":[1]}}""",
      """{"job":{"environment":["NO_EQUALS"]}}"""
    ).foreach { template =>
      withClue(s"'$template': ") {
        an[InvalidSlurmJobJson] should be thrownBy SlurmJobRequest.build(
          template,
          9,
          Map.empty,
          Map.empty
        )
      }
    }
  }

  "SlurmUnitAddress.resolve" should "substitute the node into the template" in {
    SlurmUnitAddress.resolve("{node}:8085", "node-1") shouldBe "node-1:8085"
    SlurmUnitAddress.resolve("{node}.hpc.example.edu:9000", "n2") shouldBe "n2.hpc.example.edu:9000"
  }

  private def credentials(userName: String = "alice", token: String = "aaa.bbb.ccc") =
    SlurmCredentials.parse(Some(userName), Some(token))

  private val tokenRefused = UnixSocketHttpResponse(
    511,
    """{"errors":[{"error_number":1007,"error":"Protocol authentication error"}]}"""
  )

  private def recording(answer: UnixSocketHttpResponse) = {
    var seen = List.empty[Seq[(String, String)]]
    val client = new SlurmRestClient(
      (_, _, _, headers) => {
        seen = seen :+ headers
        answer
      },
      "v0.0.42"
    )
    (client, () => seen)
  }

  "SlurmCredentials.parse" should "mean no credentials when both are blank" in {
    SlurmCredentials.parse(None, None) shouldBe None
    SlurmCredentials.parse(Some("  "), Some("")) shouldBe None
  }

  it should "require both" in {
    an[InvalidSlurmCredentials] should be thrownBy SlurmCredentials.parse(Some("alice"), None)
    an[InvalidSlurmCredentials] should be thrownBy SlurmCredentials.parse(None, Some("a.b.c"))
  }

  // Both go verbatim into headers of a hand-written HTTP request.
  it should "refuse anything that could add a header or smuggle a request" in {
    Seq(
      ("alice\r\nX-Injected: 1", "a.b.c"),
      ("alice", "a.b.c\r\nX-Injected: 1"),
      ("alice\nX-Injected: 1", "a.b.c"),
      ("al ice", "a.b.c"),
      ("alice", "a b c")
    ).foreach {
      case (userName, token) =>
        an[InvalidSlurmCredentials] should be thrownBy
          SlurmCredentials.parse(Some(userName), Some(token))
    }
  }

  it should "not repeat what was entered when refusing it" in {
    val secret = "not a token"
    val thrown = the[InvalidSlurmCredentials] thrownBy
      SlurmCredentials.parse(Some("alice"), Some(secret))
    thrown.getMessage should not include secret
  }

  it should "keep the token out of its string form" in {
    credentials().get.toString should not include "aaa.bbb.ccc"
  }

  "the client" should "send credentials on every kind of request" in {
    val expected = Seq("X-SLURM-USER-NAME" -> "alice", "X-SLURM-USER-TOKEN" -> "aaa.bbb.ccc")
    val (client, seen) = recording(UnixSocketHttpResponse(200, """{"job_id":1,"status":[]}"""))
    client.submitJob(Json.obj("job" -> Json.obj()), credentials())
    client.lookupJob(1, credentials())
    client.cancelJob(1, credentials())
    seen() shouldBe Seq(expected, expected, expected)
  }

  it should "send no headers without credentials" in {
    val (client, seen) = recording(UnixSocketHttpResponse(200, """{"job_id":1,"status":[]}"""))
    client.submitJob(Json.obj("job" -> Json.obj()))
    client.lookupJob(1)
    client.cancelJob(1)
    seen().flatten shouldBe empty
  }

  // Real answers: slurmrestd passes the token through, and slurmctld refuses it.
  it should "report a refused token on submit, as what it is" in {
    val (client, _) = recording(tokenRefused)
    a[SlurmTokenRejected] should be thrownBy
      client.submitJob(Json.obj("job" -> Json.obj()), credentials())
  }

  it should "report a refused token on cancel, as what it is" in {
    val (client, _) = recording(tokenRefused)
    a[SlurmTokenRejected] should be thrownBy client.cancelJob(1, credentials())
  }

  // Reading this as a gone job would retire a unit whose job is still running.
  it should "not take a refused token to mean the job is gone" in {
    val (client, _) = recording(tokenRefused)
    client.lookupJob(1, credentials()) shouldBe SlurmTokenUnusable
  }
}
