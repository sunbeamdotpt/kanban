/**
 * Lightweight ConnectRPC-compatible JSON handler for Hono.
 *
 * Each RPC is a POST to /<package>.<Service>/<Method> with JSON body.
 * This follows the Connect protocol (unary, JSON encoding).
 * Client uses @connectrpc/connect-web with Connect transport.
 */

import type { Context } from "hono";
import type { SessionInfo } from "./auth.ts";

export interface RpcContext {
  identity: SessionInfo;
  raw: Context;
}

type RpcHandler<Req, Res> = (req: Req, ctx: RpcContext) => Promise<Res>;

/**
 * Wraps an RPC handler into a Hono handler.
 * Parses JSON request, calls the handler, returns JSON response with Connect headers.
 */
export function rpc<Req, Res>(handler: RpcHandler<Req, Res>) {
  return async (c: Context): Promise<Response> => {
    const identity = c.get("identity") as SessionInfo;
    if (!identity) {
      return c.json({ code: "unauthenticated", message: "Not authenticated" }, 401);
    }

    let body: Req;
    try {
      body = await c.req.json() as Req;
    } catch {
      body = {} as Req;
    }

    try {
      const result = await handler(body, { identity, raw: c });
      return c.json(result);
    } catch (err) {
      if (err instanceof RpcError) {
        const status = CODE_TO_HTTP[err.code] ?? 500;
        return c.json({ code: err.code, message: err.message }, status);
      }
      console.error("RPC error:", err);
      return c.json({ code: "internal", message: "Internal server error" }, 500);
    }
  };
}

/** Standard Connect error codes. */
export type ConnectCode =
  | "canceled"
  | "unknown"
  | "invalid_argument"
  | "deadline_exceeded"
  | "not_found"
  | "already_exists"
  | "permission_denied"
  | "resource_exhausted"
  | "failed_precondition"
  | "aborted"
  | "out_of_range"
  | "unimplemented"
  | "internal"
  | "unavailable"
  | "data_loss"
  | "unauthenticated";

const CODE_TO_HTTP: Record<string, number> = {
  canceled: 499,
  unknown: 500,
  invalid_argument: 400,
  deadline_exceeded: 504,
  not_found: 404,
  already_exists: 409,
  permission_denied: 403,
  resource_exhausted: 429,
  failed_precondition: 412,
  aborted: 409,
  out_of_range: 400,
  unimplemented: 501,
  internal: 500,
  unavailable: 503,
  data_loss: 500,
  unauthenticated: 401,
};

export class RpcError extends Error {
  code: ConnectCode;
  constructor(code: ConnectCode, message: string) {
    super(message);
    this.code = code;
  }
}
