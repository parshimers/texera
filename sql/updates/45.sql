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

\c texera_db

SET search_path TO texera_db;

-- Adds the 'slurm' computing-unit type: a computing unit that runs as a Slurm batch job on
-- a compute node, with its host:port recorded in workflow_computing_unit.uri once the job
-- starts. Complements 'local' and 'kubernetes'; nothing about the existing two changes.
--
-- ALTER TYPE ... ADD VALUE cannot run inside a transaction block in PostgreSQL versions
-- before 12, and even where it is permitted the new value is unusable in the same
-- transaction. So this statement is deliberately left outside BEGIN/COMMIT, unlike the
-- other update scripts.
ALTER TYPE workflow_computing_unit_type_enum ADD VALUE IF NOT EXISTS 'slurm';
