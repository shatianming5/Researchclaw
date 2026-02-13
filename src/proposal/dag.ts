import type { PlanDag } from "./schema.js";

export type DagValidationResult = { ok: true; order: string[] } | { ok: false; errors: string[] };

export function validateDag(dag: PlanDag): DagValidationResult {
  const errors: string[] = [];
  const ids = dag.nodes.map((n) => n.id);
  const idSet = new Set<string>();
  for (const id of ids) {
    if (idSet.has(id)) {
      errors.push(`Duplicate node id: ${id}`);
    }
    idSet.add(id);
  }

  const inDegree = new Map<string, number>();
  const outgoing = new Map<string, string[]>();
  for (const id of idSet) {
    inDegree.set(id, 0);
    outgoing.set(id, []);
  }

  for (const edge of dag.edges) {
    if (!idSet.has(edge.from)) {
      errors.push(`Edge references missing node: from=${edge.from}`);
      continue;
    }
    if (!idSet.has(edge.to)) {
      errors.push(`Edge references missing node: to=${edge.to}`);
      continue;
    }
    outgoing.get(edge.from)?.push(edge.to);
    inDegree.set(edge.to, (inDegree.get(edge.to) ?? 0) + 1);
  }

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  const queue: string[] = [];
  for (const [id, deg] of inDegree.entries()) {
    if (deg === 0) {
      queue.push(id);
    }
  }
  queue.sort();

  const order: string[] = [];
  while (queue.length > 0) {
    const id = queue.shift();
    if (!id) {
      break;
    }
    order.push(id);
    for (const next of outgoing.get(id) ?? []) {
      const deg = (inDegree.get(next) ?? 0) - 1;
      inDegree.set(next, deg);
      if (deg === 0) {
        queue.push(next);
        queue.sort();
      }
    }
  }

  if (order.length !== idSet.size) {
    const remaining = [...inDegree.entries()].filter(([, deg]) => deg > 0).map(([id]) => id);
    return {
      ok: false,
      errors: [`DAG contains a cycle (remaining nodes: ${remaining.join(", ")})`],
    };
  }

  return { ok: true, order };
}
