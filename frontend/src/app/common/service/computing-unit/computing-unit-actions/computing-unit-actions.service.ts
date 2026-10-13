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
import { escape as escapeHtml } from "lodash-es";
import { map, Observable, switchMap } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { ShareAccessComponent } from "../../../../dashboard/component/user/share-access/share-access.component";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { DashboardWorkflowComputingUnit, WorkflowComputingUnitType } from "../../../type/workflow-computing-unit";
import { NotificationService } from "../../notification/notification.service";
import {
  buildLocalComputingUnitUri,
  getJvmMemorySliderConfig,
  unitTypeMessageTemplate,
} from "../../../util/computing-unit.util";
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";
import { extractErrorMessage } from "../../../util/error";

export interface StartComputingUnitRequest {
  type: WorkflowComputingUnitType;
  name: string;
  cpu: string;
  memory: string;
  gpu: string;
  jvmMemorySize: string;
  shmSize: string;
  localUri: string;
  /** A curated image to start from. Absent means the deployment's own image. */
  imageId?: number;
}

@Injectable({
  providedIn: "root",
})
export class ComputingUnitActionsService {
  // Which backend to use when the deployment offers more than one. Ordered by how much the
  // deployment is expected to know about placement: a scheduler beats a namespace beats the
  // machine the browser happens to be talking to.
  private static readonly TYPE_PREFERENCE: WorkflowComputingUnitType[] = ["slurm", "kubernetes", "local"];

  constructor(
    private modalService: NzModalService,
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService,
    private computingUnitStatusService: ComputingUnitStatusService
  ) {}

  /**
   * Create a computing unit without asking the user anything.
   *
   * The shape of a unit is a property of the deployment, not a choice the user should have to
   * make: the type comes from what the server says is enabled, and each limit is the first
   * option the server offers for that type. A deployment that wants one shape publishes one
   * option.
   */
  provision(name: string): Observable<DashboardWorkflowComputingUnit> {
    return this.computingUnitService.getComputingUnitTypes().pipe(
      map(({ typeOptions }) => {
        const type = ComputingUnitActionsService.TYPE_PREFERENCE.find(t => typeOptions.includes(t)) ?? typeOptions[0];
        if (!type) {
          throw new Error("No computing unit type is enabled for this deployment");
        }
        return type;
      }),
      switchMap(type =>
        this.computingUnitService.getComputingUnitLimitOptions(type).pipe(
          switchMap(({ cpuLimitOptions, memoryLimitOptions, gpuLimitOptions }) => {
            const memory = memoryLimitOptions[0] ?? "2Gi";
            const isKubernetes = type === "kubernetes";
            return this.create({
              type,
              name,
              cpu: cpuLimitOptions[0] ?? "1",
              memory,
              gpu: gpuLimitOptions[0] ?? "0",
              jvmMemorySize: isKubernetes ? getJvmMemorySliderConfig(memory).selectedJvmMemorySize : "NaN",
              shmSize: isKubernetes ? "64Mi" : "NaN",
              localUri: buildLocalComputingUnitUri(document.baseURI),
            });
          })
        )
      )
    );
  }

  openShareAccessModal(cuid: number, inWorkspace: boolean = true): void {
    this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: true,
        type: "computing-unit",
        id: cuid,
        inWorkspace,
      },
      nzFooter: null,
      nzTitle: "Share this computing unit with others",
      nzCentered: true,
      nzWidth: "800px",
    });
  }

  create(request: StartComputingUnitRequest): Observable<DashboardWorkflowComputingUnit> {
    if (request.type === "kubernetes") {
      return this.computingUnitService.createKubernetesBasedComputingUnit(
        request.name,
        request.cpu,
        request.memory,
        request.gpu,
        request.jvmMemorySize,
        request.shmSize,
        request.imageId
      );
    }

    if (request.type === "slurm") {
      return this.computingUnitService.createSlurmComputingUnit(
        request.name,
        request.cpu,
        request.memory,
        request.gpu
      );
    }

    if (request.type === "local") {
      return this.computingUnitService.createLocalComputingUnit(request.name, request.localUri);
    }

    throw new Error("Unsupported computing unit type");
  }

  /** Runs `onTerminated` once the unit is terminated, and not if the modal is cancelled or the termination fails. */
  confirmAndTerminate(cuid: number, unit: DashboardWorkflowComputingUnit, onTerminated?: () => void): void {
    if (!unit.computingUnit.uri) {
      this.notificationService.error("Invalid computing unit.");
      return;
    }

    // ng-zorro renders this string as HTML and any user can name their unit, so the name is escaped.
    const unitName = escapeHtml(unit.computingUnit.name);
    const unitType = unit?.computingUnit.type || "kubernetes"; // fallback
    const templates = unitTypeMessageTemplate[unitType];

    // Show confirmation modal
    this.modalService.confirm({
      nzTitle: templates.terminateTitle,
      nzContent: templates.terminateWarning
        ? `
      <p>Are you sure you want to terminate <strong>${unitName}</strong>?</p>
      ${templates.terminateWarning}
    `
        : `
      <p>Are you sure you want to disconnect from <strong>${unitName}</strong>?</p>
    `,
      nzOkText: unitType === "local" ? "Disconnect" : "Terminate",
      nzOkType: "primary",
      nzOnOk: () => {
        // Use the ComputingUnitStatusService to handle termination
        // This will properly close the websocket before terminating the unit
        this.computingUnitStatusService.terminateComputingUnit(cuid).subscribe({
          next: (success: boolean) => {
            if (success) {
              this.notificationService.success(`Terminated Computing Unit: ${unitName}`);
              onTerminated?.();
            } else {
              this.notificationService.error("Failed to terminate computing unit");
            }
          },
          error: (err: unknown) => {
            this.notificationService.error(`Failed to terminate computing unit: ${extractErrorMessage(err)}`);
          },
        });
      },
      nzCancelText: "Cancel",
    });
  }
}
