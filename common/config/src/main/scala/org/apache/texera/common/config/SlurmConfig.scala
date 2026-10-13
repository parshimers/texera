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

package org.apache.texera.common.config

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Settings for computing units that run as Slurm batch jobs.
  *
  * Shaped after [[KubernetesConfig]]: one object of vals read once from its own resource file,
  * every key overridable from the environment so a deployment can be configured without
  * rebuilding.
  */
object SlurmConfig {

  private val conf: Config = ConfigFactory.parseResources("slurm.conf").resolve()

  // Comma-separated option lists are given as a single string so one environment variable can
  // carry the whole list.
  private def csv(key: String): List[String] =
    conf.getString(key).split(",").map(_.trim).filter(_.nonEmpty).toList

  val slurmComputingUnitEnabled: Boolean = conf.getBoolean("slurm.enabled")

  val partition: String = conf.getString("slurm.partition")

  // Empty means "use the user's default account", i.e. omit --account entirely. An empty
  // string is not the same as an absent one: a bare `-A` is a submission error.
  val account: Option[String] =
    Option(conf.getString("slurm.account")).map(_.trim).filter(_.nonEmpty)

  val timeLimit: String = conf.getString("slurm.time-limit")

  val advertiseHost: String = conf.getString("slurm.advertise-host")

  val stateDir: String = conf.getString("slurm.state-dir")

  val texeraHome: String = conf.getString("slurm.texera-home")

  val launcher: String = conf.getString("slurm.launcher")

  val envPassthrough: List[String] = csv("slurm.env-passthrough")

  val pathPrefix: Option[String] =
    Option(conf.getString("slurm.path-prefix")).map(_.trim).filter(_.nonEmpty)

  val jvmHeapFraction: Double = conf.getDouble("slurm.jvm-heap-fraction")

  val cpuLimitOptions: List[String] = csv("slurm.computing-unit-cpu-limit-options")

  val memoryLimitOptions: List[String] = csv("slurm.computing-unit-memory-limit-options")

  val gpuLimitOptions: List[String] = csv("slurm.computing-unit-gpu-limit-options")

  val gpuResourceName: String = conf.getString("slurm.computing-unit-gpu-resource-name")

  val maxNumOfRunningComputingUnitsPerUser: Int =
    conf.getInt("slurm.max-num-of-running-computing-units-per-user")

  val sbatchPath: String = conf.getString("slurm.sbatch")
  val squeuePath: String = conf.getString("slurm.squeue")
  val scancelPath: String = conf.getString("slurm.scancel")
}
