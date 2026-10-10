import { describe, expect, it } from "vitest";
import {
  initialProjection,
  projectionError,
  updateProjection,
} from "./fire-state";

describe("live stop-work age editing", () => {
  it("keeps the first digit while pausing invalid calculations", () => {
    const partial = updateProjection(initialProjection, "stop_working_age", 3);
    expect(partial.stop_working_age).toBe(3);
    expect(projectionError(partial)).not.toBeNull();
    const complete = updateProjection(partial, "stop_working_age", 35);
    expect(complete.stop_working_age).toBe(35);
    expect(projectionError(complete)).toBeNull();
  });

  it("preserves partial input during unrelated edits", () => {
    const partial = { ...initialProjection, stop_working_age: 3 };
    const changed = updateProjection(partial, "monthly_savings", 500.25);
    expect(changed.stop_working_age).toBe(3);
    expect(projectionError(changed)).not.toBeNull();
  });

  it("clears an incompatible stop age when the horizon changes", () => {
    const current = { ...initialProjection, stop_working_age: 65 };
    const shorter = updateProjection(current, "years", 20);
    expect(shorter.stop_working_age).toBeNull();
    expect(projectionError(shorter)).toBeNull();
  });
});
