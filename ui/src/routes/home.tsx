/**
 * HomePage — "My projects" landing page.
 * TODO(Stage 6e): Replace with actual project list and board grid.
 */

export function HomePage() {
  return (
    <div className="p-6">
      <h1 className="text-4xl font-bold mb-4">Projects</h1>
      <a href="/p/test-project" className="text-accent underline">
        Test Project
      </a>
    </div>
  );
}
