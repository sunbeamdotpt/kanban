import { assertEquals } from "https://deno.land/std@0.220.0/assert/mod.ts";
import {
  Visibility,
  Role,
  Priority,
  visibilityFromString,
  roleFromString,
  priorityFromString,
  VISIBILITY_LABELS,
  ROLE_LABELS,
  PRIORITY_LABELS,
} from "../../gen/kanban/v1/kanban_pb.ts";

Deno.test("visibilityFromString — known values", () => {
  assertEquals(visibilityFromString("private"), Visibility.PRIVATE);
  assertEquals(visibilityFromString("members"), Visibility.MEMBERS);
  assertEquals(visibilityFromString("public"), Visibility.PUBLIC);
});

Deno.test("visibilityFromString — unknown returns UNSPECIFIED", () => {
  assertEquals(visibilityFromString("foobar"), Visibility.UNSPECIFIED);
  assertEquals(visibilityFromString(""), Visibility.UNSPECIFIED);
});

Deno.test("roleFromString — known values", () => {
  assertEquals(roleFromString("viewer"), Role.VIEWER);
  assertEquals(roleFromString("editor"), Role.EDITOR);
  assertEquals(roleFromString("admin"), Role.ADMIN);
  assertEquals(roleFromString("owner"), Role.OWNER);
});

Deno.test("roleFromString — unknown returns UNSPECIFIED", () => {
  assertEquals(roleFromString("superadmin"), Role.UNSPECIFIED);
});

Deno.test("priorityFromString — known values", () => {
  assertEquals(priorityFromString("low"), Priority.LOW);
  assertEquals(priorityFromString("medium"), Priority.MEDIUM);
  assertEquals(priorityFromString("high"), Priority.HIGH);
  assertEquals(priorityFromString("critical"), Priority.CRITICAL);
});

Deno.test("priorityFromString — unknown returns UNSPECIFIED", () => {
  assertEquals(priorityFromString("urgent"), Priority.UNSPECIFIED);
});

Deno.test("VISIBILITY_LABELS — all values mapped", () => {
  assertEquals(VISIBILITY_LABELS[Visibility.PRIVATE], "private");
  assertEquals(VISIBILITY_LABELS[Visibility.MEMBERS], "members");
  assertEquals(VISIBILITY_LABELS[Visibility.PUBLIC], "public");
  assertEquals(VISIBILITY_LABELS[Visibility.UNSPECIFIED], "unspecified");
});

Deno.test("ROLE_LABELS — all values mapped", () => {
  assertEquals(ROLE_LABELS[Role.VIEWER], "viewer");
  assertEquals(ROLE_LABELS[Role.EDITOR], "editor");
  assertEquals(ROLE_LABELS[Role.ADMIN], "admin");
  assertEquals(ROLE_LABELS[Role.OWNER], "owner");
});

Deno.test("PRIORITY_LABELS — all values mapped", () => {
  assertEquals(PRIORITY_LABELS[Priority.LOW], "low");
  assertEquals(PRIORITY_LABELS[Priority.MEDIUM], "medium");
  assertEquals(PRIORITY_LABELS[Priority.HIGH], "high");
  assertEquals(PRIORITY_LABELS[Priority.CRITICAL], "critical");
});

Deno.test("Enum values are correct numbers", () => {
  assertEquals(Visibility.UNSPECIFIED, 0);
  assertEquals(Visibility.PRIVATE, 1);
  assertEquals(Role.VIEWER, 1);
  assertEquals(Role.OWNER, 4);
  assertEquals(Priority.CRITICAL, 4);
});
