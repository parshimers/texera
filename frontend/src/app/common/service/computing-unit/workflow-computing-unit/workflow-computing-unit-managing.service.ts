/**
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

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable } from "rxjs";
import { AppSettings } from "../../../app-setting";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnit,
  WorkflowComputingUnitResourceLimit,
  WorkflowComputingUnitType,
} from "../../../type/workflow-computing-unit";
import { map } from "rxjs/operators";

export const COMPUTING_UNIT_BASE_URL = "computing-unit";
export const COMPUTING_UNIT_CREATE_URL = `${COMPUTING_UNIT_BASE_URL}/create`;
export const COMPUTING_UNIT_LIST_URL = `${COMPUTING_UNIT_BASE_URL}`;
export const COMPUTING_UNIT_TYPES_URL = `${COMPUTING_UNIT_BASE_URL}/types`;

@Injectable({
  providedIn: "root",
})
export class WorkflowComputingUnitManagingService {
  constructor(private http: HttpClient) {}

  /** Ensure the `resource` field is parsed into an object. */
  private parseDashboardUnit = (raw: DashboardWorkflowComputingUnit): DashboardWorkflowComputingUnit => {
    const cu = raw.computingUnit as WorkflowComputingUnit & {
      resource: string | WorkflowComputingUnitResourceLimit;
    };

    if (typeof cu.resource === "string") {
      try {
        cu.resource = JSON.parse(cu.resource) as WorkflowComputingUnitResourceLimit;
      } catch {
        // fall back to an empty object, so the UI never crashes
        cu.resource = {
          cpuLimit: "NaN",
          memoryLimit: "NaN",
          gpuLimit: "NaN",
          jvmMemorySize: "NaN",
          shmSize: "NaN",
          nodeAddresses: [],
        };
      }
    }
    return { ...raw, computingUnit: cu };
  };

  /**
   * Create a new workflow computing unit (pod).
   * @param name The name for the computing unit.
   * @param cpuLimit The cpu resource limit for the computing unit.
   * @param memoryLimit The memory resource limit for the computing unit.
   * @param gpuLimit The gpu resource limit for the computing unit.
   * @param jvmMemorySize The JVM memory size (e.g. "1G", "2G")
   * @param unitType The type of computing unit (e.g. "local", "kubernetes")
   * @param shmSize The shared memory size
   * @param uri The URI of the local computing unit; for kubernetes-based computing units, this is not used in the backend.
   * @returns An Observable of the created WorkflowComputingUnit.
   */
  private createComputingUnit(
    name: string,
    cpuLimit: string,
    memoryLimit: string,
    gpuLimit: string,
    jvmMemorySize: string,
    shmSize: string,
    uri: string,
    unitType: WorkflowComputingUnitType
  ): Observable<DashboardWorkflowComputingUnit> {
    const body = { name, cpuLimit, memoryLimit, gpuLimit, jvmMemorySize, shmSize, uri, unitType };

    return this.http
      .post<DashboardWorkflowComputingUnit>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_CREATE_URL}`, body)
      .pipe(map(raw => this.parseDashboardUnit(raw)));
  }

  /**
   * Create a new Kubernetes-based workflow computing unit.
   *
   * @param name The name for the computing unit.
   * @param cpuLimit The cpu resource limit for the computing unit.
   * @param memoryLimit The memory resource limit for the computing unit.
   * @param gpuLimit The gpu resource limit for the computing unit.
   * @param jvmMemorySize The JVM memory size (e.g. "1G", "2G")
   * @param shmSize The shared memory size
   * @returns An Observable of the created WorkflowComputingUnit.
   */
  public createKubernetesBasedComputingUnit(
    name: string,
    cpuLimit: string,
    memoryLimit: string,
    gpuLimit: string,
    jvmMemorySize: string,
    shmSize: string
  ): Observable<DashboardWorkflowComputingUnit> {
    return this.createComputingUnit(name, cpuLimit, memoryLimit, gpuLimit, jvmMemorySize, shmSize, "", "kubernetes");
  }

  /**
   * Create a new local workflow computing unit.
   *
   * @param name The name of the computing unit.
   * @param uri The URI of the local computing unit.
   * @returns An Observable of the created WorkflowComputingUnit.
   */
  public createLocalComputingUnit(name: string, uri: string): Observable<DashboardWorkflowComputingUnit> {
    return this.createComputingUnit(name, "NaN", "NaN", "NaN", "NaN", "NaN", uri, "local");
  }

  /**
   * Create a new Slurm-backed workflow computing unit.
   *
   * The scheduler decides where it runs and the job publishes its address once it starts, so
   * there is no URI to supply here; the JVM-heap and shared-memory knobs are Kubernetes-only.
   *
   * @param name The name of the computing unit.
   * @param cpuLimit The cpu resource limit for the computing unit.
   * @param memoryLimit The memory resource limit for the computing unit.
   * @param gpuLimit The gpu resource limit for the computing unit.
   * @returns An Observable of the created WorkflowComputingUnit.
   */
  public createSlurmComputingUnit(
    name: string,
    cpuLimit: string,
    memoryLimit: string,
    gpuLimit: string
  ): Observable<DashboardWorkflowComputingUnit> {
    return this.createComputingUnit(name, cpuLimit, memoryLimit, gpuLimit, "NaN", "NaN", "", "slurm");
  }

  /**
   * Terminate a computing unit (pod) by its URI.
   * @returns An Observable of the server response.
   * @param cuid
   */
  public terminateComputingUnit(cuid: number): Observable<Response> {
    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_BASE_URL}/${cuid}/terminate`);
  }

  /**
   * Fetch the list of available CPU and memory limit options.
   *
   * The options depend on the backend that will run the unit -- a Slurm unit is sized in whole
   * nodes, a pod in millicores -- so the type is passed through. Omitting it keeps the server's
   * own default.
   *
   * @param unitType The computing unit type whose option lists are wanted.
   * @returns An Observable containing both CPU and memory limit options.
   */
  public getComputingUnitLimitOptions(unitType?: string): Observable<{
    cpuLimitOptions: string[];
    memoryLimitOptions: string[];
    gpuLimitOptions: string[];
  }> {
    const query = unitType ? `?type=${encodeURIComponent(unitType)}` : "";
    return this.http.get<{
      cpuLimitOptions: string[];
      memoryLimitOptions: string[];
      gpuLimitOptions: string[];
    }>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_BASE_URL}/limits${query}`);
  }

  /**
   * Fetch the list of supported computing unit types.
   * @returns An Observable containing the available computing unit types.
   */
  public getComputingUnitTypes(): Observable<{
    typeOptions: WorkflowComputingUnitType[];
  }> {
    return this.http.get<{
      typeOptions: WorkflowComputingUnitType[];
    }>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_TYPES_URL}`);
  }

  /**
   * List all active computing units.
   * @returns An Observable of a list of DashboardWorkflowComputingUnit.
   */
  public listComputingUnits(): Observable<DashboardWorkflowComputingUnit[]> {
    return this.http
      .get<DashboardWorkflowComputingUnit[]>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_LIST_URL}`)
      .pipe(map(arr => arr.map(unit => this.parseDashboardUnit(unit))));
  }

  public getComputingUnit(cuid: number): Observable<DashboardWorkflowComputingUnit> {
    return this.http
      .get<DashboardWorkflowComputingUnit>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_BASE_URL}/${cuid}`)
      .pipe(map(raw => this.parseDashboardUnit(raw)));
  }

  /**
   * Rename a computing unit.
   * @param cuid The ID of the computing unit to rename.
   * @param name The new name for the computing unit.
   * @returns An Observable of the server response.
   */
  public renameComputingUnit(cuid: number, name: string): Observable<Response> {
    return this.http.put<Response>(
      `${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_BASE_URL}/${cuid}/rename/${encodeURIComponent(name)}`,
      {}
    );
  }
}
