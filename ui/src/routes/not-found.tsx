/**
 * NotFound — 404 page for unmatched routes.
 */

import { Link } from "react-router";

export function NotFound() {
  return (
    <div className="flex items-center justify-center min-h-screen">
      <div className="text-center">
        <h1 className="text-6xl font-bold mb-4">404</h1>
        <p className="text-lg text-muted mb-6">Page not found</p>
        <Link to="/" className="text-accent underline">
          Return to home
        </Link>
      </div>
    </div>
  );
}
