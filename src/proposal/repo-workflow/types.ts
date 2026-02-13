import type { ExecuteNodeStatus } from "../execute/types.js";

export type RepoRef = {
  repoRel: string;
  repoKey: string;
};

export type RepoWorktreeRecord = RepoRef & {
  createdAt: string;
  baseRepoAbs: string;
  worktreeRel: string;
  worktreeAbs: string;
  branchName: string;
  baseSha: string;
};

export type RepoExecutedNode = {
  nodeId: string;
  status?: ExecuteNodeStatus;
};

export type RepoWorkflowEvidence = {
  repoKey: string;
  repoRel: string;
  remoteUrl?: string;
  branchName: string;
  baseSha: string;
  headSha?: string;
  dirtyAfter?: boolean;
  worktreeRel: string;
  evidenceDirRel: string;
  outputs: {
    repoJson: string;
    diffPatch: string;
    diffStat: string;
    prBody: string;
    reproScript: string;
  };
};

export type RepoWorkflowManifest = {
  planId: string;
  createdAt: string;
  repos: RepoWorkflowEvidence[];
};
