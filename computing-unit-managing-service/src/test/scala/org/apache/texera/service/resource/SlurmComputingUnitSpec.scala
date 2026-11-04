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

package org.apache.texera.service.resource

import jakarta.ws.rs.ForbiddenException
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  UserRoleEnum,
  WorkflowComputingUnitTerminationReasonEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}
import org.apache.texera.service.resource.ComputingUnitManagingResource.{
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitCreationParams
}
import org.apache.texera.service.util.{
  InsufficientComputingUnitQuota,
  InvalidSlurmJobJson,
  SlurmRequestFailure,
  SlurmRestClient,
  UnixSocketHttpResponse
}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, LoneElement}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{JsObject, Json}

import java.io.IOException
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

// Drives the endpoints against the embedded database with a stand-in for slurmrestd, answering
// with the bodies a real one returned. bin/k8s/README.md says how to exercise a real one.
class SlurmComputingUnitSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with LoneElement
    with MockTexeraDB {

  private val jobJson =
    """{"job":{"partition":"debug","environment":["PATH=/usr/bin","S3=override"],"script":"x"}}"""

  private def user(id: Int, role: UserRoleEnum = UserRoleEnum.REGULAR): User = {
    val u = new User()
    u.setUid(id)
    u.setName(s"user-$id")
    u.setEmail(s"user-$id@example.com")
    u.setRole(role)
    u
  }
  private lazy val owner = new SessionUser(user(1000))
  private lazy val admin = new SessionUser(user(1001, UserRoleEnum.ADMIN))
  private lazy val stranger = new SessionUser(user(1002))

  /** Scripted slurmrestd. `submitted` holds what each submission really put on the wire. */
  private class FakeSlurm {
    val requests: ListBuffer[(String, String)] = ListBuffer.empty
    val submitted: ListBuffer[JsObject] = ListBuffer.empty
    var nextJobId = 100L
    var submit: () => UnixSocketHttpResponse = () => {
      nextJobId += 1
      UnixSocketHttpResponse(200, s"""{"job_id":$nextJobId,"errors":[],"warnings":[]}""")
    }
    var lookup: () => UnixSocketHttpResponse = () => job("PENDING", reason = "Priority")
    // Overrides `lookup` for one job, for the tests where units' jobs differ.
    var lookupOf: Map[String, () => UnixSocketHttpResponse] = Map.empty
    var cancel: () => UnixSocketHttpResponse =
      () => UnixSocketHttpResponse(200, """{"status":[],"errors":[],"warnings":[]}""")

    val client = new SlurmRestClient(
      (method, path, body) => {
        requests += ((method, path))
        method match {
          case "POST" =>
            submitted += Json.parse(body.get).as[JsObject]
            submit()
          case "GET" => lookupOf.getOrElse(path.split("/").last, lookup)()
          case _     => cancel()
        }
      },
      "v0.0.42"
    )
  }

  private def job(state: String, host: String = "", reason: String = "None") =
    UnixSocketHttpResponse(
      200,
      s"""{"jobs":[{"job_state":["$state"],"batch_host":"$host","state_reason":"$reason"}],
         |"errors":[],"warnings":[]}""".stripMargin
    )
  private val jobGone = UnixSocketHttpResponse(
    404,
    """{"jobs":[],"errors":[{"error_number":2017,"error":"Invalid job id specified"}]}"""
  )

  private var slurm: FakeSlurm = _

  private def resource(
      enabled: Boolean = true,
      template: String = jobJson,
      fake: => FakeSlurm = slurm
  ) =
    new ComputingUnitManagingResource(
      (_, _) => (),
      enabled,
      template,
      fake.client,
      Map("S3" -> "from-manager", "JDBC" -> "jdbc:manager")
    )

  private def params(name: String = "hpc") =
    WorkflowComputingUnitCreationParams(name, "slurm", "NaN", "NaN", "NaN", "NaN", "NaN")

  private def dao = new WorkflowComputingUnitDao(getDSLContext.configuration())

  private def stored(unit: DashboardWorkflowComputingUnit): WorkflowComputingUnit =
    dao.fetchOneByCuid(unit.computingUnit.getCuid)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val users = new UserDao(getDSLContext.configuration())
    users.insert(owner.getUser)
    users.insert(admin.getUser)
    users.insert(stranger.getUser)
  }

  override protected def afterAll(): Unit =
    try shutdownDB()
    finally super.afterAll()

  override protected def beforeEach(): Unit = {
    slurm = new FakeSlurm
    dao.findAll().asScala.foreach(dao.delete(_))
  }

  "getComputingUnitTypes" should "offer slurm only when it is enabled" in {
    resource(enabled = true).getComputingUnitTypes(owner).typeOptions should contain("slurm")
    resource(enabled = false).getComputingUnitTypes(owner).typeOptions should not contain "slurm"
  }

  "createWorkflowComputingUnit" should "reject slurm while it is disabled" in {
    val thrown = the[ForbiddenException] thrownBy
      resource(enabled = false).createWorkflowComputingUnit(params(), owner)
    thrown.getMessage should include("Unit type 'slurm' is not allowed")
    slurm.requests shouldBe empty
    dao.findAll() shouldBe empty
  }

  it should "submit the administrator's job JSON and record the job" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)

    slurm.requests.head shouldBe (("POST", "/slurm/v0.0.42/job/submit"))
    val cuid = created.computingUnit.getCuid
    val job = (slurm.submitted.head \ "job").as[JsObject]
    (job \ "partition").as[String] shouldBe "debug"
    (job \ "script").as[String] shouldBe "x"
    (job \ "name").as[String] shouldBe s"texera-computing-unit-$cuid"
    val environment = (job \ "environment").as[Seq[String]]
    environment should contain("PATH=/usr/bin")
    environment should contain("JDBC=jdbc:manager")
    environment should contain("S3=override")
    environment should not contain "S3=from-manager"
    environment should contain(s"TEXERA_CU_ID=$cuid")
    environment.exists(_.startsWith("USER_JWT_TOKEN=")) shouldBe true

    val unit = stored(created)
    unit.getType shouldBe WorkflowComputingUnitTypeEnum.slurm
    (Json.parse(unit.getResource) \ "slurmJobId").as[Long] shouldBe 101L
  }

  // The gateway refuses a unit with no recorded address, which is right until it has a node.
  it should "report a queued job as Pending with no address, and say what it waits for" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    created.status shouldBe "Pending"
    created.statusReason.get should include("Priority")
    created.isOwner shouldBe true
    stored(created).getUri shouldBe ""
  }

  it should "record the address at once if the job started immediately" in {
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val created = resource().createWorkflowComputingUnit(params(), owner)
    created.status shouldBe "Running"
    created.computingUnit.getUri shouldBe "node-1:8085"
    stored(created).getUri shouldBe "node-1:8085"
  }

  it should "leave no row behind when Slurm refuses the job" in {
    slurm.submit = () =>
      UnixSocketHttpResponse(
        500,
        """{"errors":[{"error_number":2000,"error":"Invalid partition name specified"}]}"""
      )
    val thrown = the[SlurmRequestFailure] thrownBy
      resource().createWorkflowComputingUnit(params(), owner)
    thrown.getMessage should include("Invalid partition name specified")
    dao.findAll() shouldBe empty
  }

  it should "leave no row behind and answer 503 when the socket is unreachable" in {
    slurm.submit = () => throw new IOException("Connection refused")
    val thrown = the[SlurmRequestFailure] thrownBy
      resource().createWorkflowComputingUnit(params(), owner)
    thrown.getResponse.getStatus shouldBe 503
    dao.findAll() shouldBe empty
  }

  it should "not contact Slurm when the job JSON is invalid" in {
    a[InvalidSlurmJobJson] should be thrownBy
      resource(template = "").createWorkflowComputingUnit(params(), owner)
    slurm.requests shouldBe empty
    dao.findAll() shouldBe empty
  }

  it should "reject a curated image, which a Slurm job cannot run" in {
    a[ForbiddenException] should be thrownBy
      resource().createWorkflowComputingUnit(params().copy(iid = Some(1)), owner)
    slurm.requests shouldBe empty
  }

  it should "reject a blank name" in {
    a[ForbiddenException] should be thrownBy
      resource().createWorkflowComputingUnit(params("  "), owner)
  }

  it should "count against the running-unit quota" in {
    val quota =
      org.apache.texera.common.config.KubernetesConfig.maxNumOfRunningComputingUnitsPerUser
    (0 until quota).foreach(i => resource().createWorkflowComputingUnit(params(s"u$i"), owner))
    an[InsufficientComputingUnitQuota] should be thrownBy
      resource().createWorkflowComputingUnit(params("one-more"), owner)
  }

  "listComputingUnits" should "report a pending unit without touching its row" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    val listed = resource().listComputingUnits(owner).loneElement
    listed.status shouldBe "Pending"
    stored(created).getTerminateTime shouldBe null
    stored(created).getUri shouldBe ""
  }

  it should "record the address once the job runs, without losing the job id" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.lookup = () => job("RUNNING", host = "node-7")

    val listed = resource().listComputingUnits(owner).loneElement
    listed.status shouldBe "Running"
    listed.computingUnit.getUri shouldBe "node-7:8085"

    val unit = stored(created)
    unit.getUri shouldBe "node-7:8085"
    (Json.parse(unit.getResource) \ "nodeAddresses").as[Seq[String]] shouldBe Seq("node-7:8085")
    (Json.parse(unit.getResource) \ "slurmJobId").as[Long] shouldBe 101L
  }

  it should "follow a job requeued onto another node" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.lookup = () => job("RUNNING", host = "node-1")
    resource().listComputingUnits(owner)
    slurm.lookup = () => job("RUNNING", host = "node-2")
    resource().listComputingUnits(owner)
    stored(created).getUri shouldBe "node-2:8085"
  }

  it should "show a job that ended as Failed with why, until Slurm forgets it" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.lookup = () => job("TIMEOUT")
    val listed = resource().listComputingUnits(owner).loneElement
    listed.status shouldBe "Failed"
    listed.statusReason.get should include("time limit")
    stored(created).getTerminateTime shouldBe null
  }

  it should "retire a unit whose job Slurm no longer knows" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.lookup = () => jobGone

    resource().listComputingUnits(owner) shouldBe empty
    val unit = stored(created)
    unit.getTerminateTime should not be null
    unit.getTerminationReason shouldBe WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  // Slurm being down, or a wrong api-version, says nothing about the job.
  it should "keep every unit, as Unknown, when Slurm cannot answer" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)

    slurm.lookup = () => throw new IOException("Connection refused")
    val unreachable = resource().listComputingUnits(owner).loneElement
    unreachable.status shouldBe "Unknown"
    unreachable.statusReason shouldBe defined

    slurm.lookup = () => UnixSocketHttpResponse(404, "Unable to find requested URL")
    resource().listComputingUnits(owner).loneElement.status shouldBe "Unknown"

    slurm.lookup = () => UnixSocketHttpResponse(401, "Authentication failure")
    resource().listComputingUnits(owner).loneElement.status shouldBe "Unknown"

    stored(created).getTerminateTime shouldBe null
  }

  it should "not contact Slurm for a user with no Slurm units" in {
    resource().listComputingUnits(stranger) shouldBe empty
    slurm.requests shouldBe empty
  }

  it should "keep a shared user from seeing why a unit is in its state" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    resource()
      .getComputingUnitInfo(created.computingUnit.getCuid, stranger)
      .statusReason shouldBe None
  }

  // Computing units bind fixed ports, so a node hosts one. The gateway routes on the recorded
  // address alone, so a second job's seconds alive on that node must not send its user to the
  // first user's unit.
  private def jobIdOf(unit: DashboardWorkflowComputingUnit): String =
    (Json.parse(stored(unit).getResource) \ "slurmJobId").as[Long].toString

  private lazy val other = new SessionUser(user(1003))

  "a job on a node another unit holds" should "get no address, and stay Pending saying why" in {
    new UserDao(getDSLContext.configuration()).merge(other.getUser)
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val first = resource().createWorkflowComputingUnit(params("first"), owner)
    first.computingUnit.getUri shouldBe "node-1:8085"

    val second = resource().createWorkflowComputingUnit(params("second"), other)

    second.status shouldBe "Pending"
    second.statusReason.get should include("another computing unit")
    stored(second).getUri shouldBe ""
    val listed = resource().listComputingUnits(other).loneElement
    listed.status shouldBe "Pending"
    listed.computingUnit.getUri shouldBe ""
    val info = resource().getComputingUnitInfo(second.computingUnit.getCuid, other)
    info.status shouldBe "Pending"
    stored(second).getUri shouldBe ""
  }

  it should "take the address once the holder is terminated" in {
    new UserDao(getDSLContext.configuration()).merge(other.getUser)
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val first = resource().createWorkflowComputingUnit(params("first"), owner)
    val second = resource().createWorkflowComputingUnit(params("second"), other)

    resource().terminateComputingUnit(first.computingUnit.getCuid, owner)
    val listed = resource().listComputingUnits(other).loneElement

    listed.status shouldBe "Running"
    stored(second).getUri shouldBe "node-1:8085"
  }

  // Its owner may be offline, with nothing polling that unit's row to notice the job ended.
  it should "take the address from a holder whose job has ended" in {
    new UserDao(getDSLContext.configuration()).merge(other.getUser)
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val first = resource().createWorkflowComputingUnit(params("first"), owner)
    val second = resource().createWorkflowComputingUnit(params("second"), other)

    Seq[() => UnixSocketHttpResponse](() => job("TIMEOUT"), () => jobGone).foreach { holderJob =>
      slurm.lookupOf = Map(jobIdOf(first) -> holderJob)
      resource().listComputingUnits(other).loneElement.status shouldBe "Running"
      stored(second).getUri shouldBe "node-1:8085"
      stored(first).getUri shouldBe ""

      // Back to the contested state for the next holder state.
      slurm.lookupOf = Map.empty
      dao.update({
        val unit = stored(first)
        unit.setUri("node-1:8085")
        unit
      })
      dao.update({
        val unit = stored(second)
        unit.setUri("")
        unit
      })
    }
  }

  it should "stay away from the address while the holder cannot be checked" in {
    new UserDao(getDSLContext.configuration()).merge(other.getUser)
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val first = resource().createWorkflowComputingUnit(params("first"), owner)
    val second = resource().createWorkflowComputingUnit(params("second"), other)

    slurm.lookupOf = Map(
      jobIdOf(first) -> (() => UnixSocketHttpResponse(500, "{}"))
    )
    resource().listComputingUnits(other).loneElement.status shouldBe "Pending"
    stored(second).getUri shouldBe ""
    stored(first).getUri shouldBe "node-1:8085"
  }

  "a job that stops running" should "give up its address, when requeued" in {
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val created = resource().createWorkflowComputingUnit(params(), owner)

    slurm.lookup = () => job("PENDING", reason = "BeginTime")
    val listed = resource().listComputingUnits(owner).loneElement

    listed.status shouldBe "Pending"
    listed.computingUnit.getUri shouldBe ""
    stored(created).getUri shouldBe ""
    (Json.parse(stored(created).getResource) \ "nodeAddresses").as[Seq[String]] shouldBe empty
    (Json.parse(stored(created).getResource) \ "slurmJobId").as[Long] shouldBe 101L
  }

  it should "give up its address when it ends, but keep the row showing why" in {
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val created = resource().createWorkflowComputingUnit(params(), owner)

    slurm.lookup = () => job("FAILED")
    resource().listComputingUnits(owner).loneElement.status shouldBe "Failed"

    stored(created).getUri shouldBe ""
    stored(created).getTerminateTime shouldBe null
  }

  // An unanswered lookup says nothing about the job, so a running unit keeps being reachable.
  it should "keep its address while Slurm cannot answer" in {
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.lookup = () => throw new IOException("Connection refused")
    resource().listComputingUnits(owner).loneElement.status shouldBe "Unknown"
    stored(created).getUri shouldBe "node-1:8085"
  }

  "getComputingUnitInfo" should "record the address, as the frontend polls only this once selected" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.lookup = () => job("RUNNING", host = "node-3")

    val info = resource().getComputingUnitInfo(created.computingUnit.getCuid, owner)

    info.status shouldBe "Running"
    info.computingUnit.getUri shouldBe "node-3:8085"
    stored(created).getUri shouldBe "node-3:8085"
  }

  // Reading a unit its owner already terminated must not rewrite why it ended.
  it should "leave a terminated unit's termination alone" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    val cuid = created.computingUnit.getCuid
    resource().terminateComputingUnit(cuid, owner)
    val terminatedAt = stored(created).getTerminateTime

    slurm.lookup = () => jobGone
    resource().getComputingUnitInfo(cuid, owner)

    stored(created).getTerminateTime shouldBe terminatedAt
    stored(created).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED
  }

  "terminateComputingUnit" should "cancel the job and stamp the unit" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)

    resource().terminateComputingUnit(created.computingUnit.getCuid, owner).getStatus shouldBe 200

    slurm.requests should contain(("DELETE", "/slurm/v0.0.42/job/101"))
    stored(created).getTerminateTime should not be null
    stored(created).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED
  }

  it should "terminate a pending unit, which has no address yet" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    stored(created).getUri shouldBe ""
    resource().terminateComputingUnit(created.computingUnit.getCuid, owner).getStatus shouldBe 200
    stored(created).getTerminateTime should not be null
  }

  // Stamping it anyway would hide a job still holding the allocation.
  it should "leave the unit live when Slurm cannot cancel the job" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.cancel = () => throw new IOException("Connection refused")
    a[SlurmRequestFailure] should be thrownBy
      resource().terminateComputingUnit(created.computingUnit.getCuid, owner)
    stored(created).getTerminateTime shouldBe null
  }

  it should "refuse a stranger without cancelling anything" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    resource()
      .terminateComputingUnit(created.computingUnit.getCuid, stranger)
      .getStatus shouldBe 400
    slurm.requests.map(_._1) should not contain "DELETE"
  }

  it should "free a terminated unit's address for a later job on that node" in {
    new UserDao(getDSLContext.configuration()).merge(other.getUser)
    slurm.lookup = () => job("RUNNING", host = "node-1")
    val first = resource().createWorkflowComputingUnit(params("first"), owner)
    resource().terminateComputingUnit(first.computingUnit.getCuid, owner)
    resource().createWorkflowComputingUnit(params("later"), other).status shouldBe "Running"
  }

  "getComputingUnitResourceLimit" should "report no limits, as the job JSON holds them" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    val limits =
      resource().getComputingUnitResourceLimit(created.computingUnit.getCuid.toString, owner)
    limits.cpuLimit shouldBe "NaN"
    limits.memoryLimit shouldBe "NaN"
    limits.gpuLimit shouldBe "NaN"
  }

  "the admin listing" should "reconcile and report Slurm units like the owner's listing" in {
    val created = resource().createWorkflowComputingUnit(params(), owner)
    slurm.lookup = () => job("RUNNING", host = "node-9")

    val listed =
      new AdminComputingUnitResource(slurm.client).listAllComputingUnits(admin).loneElement

    listed.status shouldBe "Running"
    stored(created).getUri shouldBe "node-9:8085"

    slurm.lookup = () => jobGone
    new AdminComputingUnitResource(slurm.client).listAllComputingUnits(admin) shouldBe empty
    stored(created).getTerminateTime should not be null
  }
}
