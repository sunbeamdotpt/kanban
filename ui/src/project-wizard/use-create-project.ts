/**
 * use-create-project.ts
 *
 * Wrapper around useRpcMutation for ProjectService.CreateProject.
 * Handles idempotency key generation and error handling.
 */

import { useRpcMutation } from "@sunbeam/g2v/hooks";
import { ProjectService } from "../gen/sunbeam/kanban/v1/projects_pb";

export interface CreateProjectParams {
  name: string;
  prefix: string;
  icon: string;
  color: string;
  idempotencyKey: string;
}

export function useCreateProject() {
  return useRpcMutation(ProjectService, "createProject");
}
