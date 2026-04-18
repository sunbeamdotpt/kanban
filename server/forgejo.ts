/**
 * Forgejo API client for cross-linking issues/PRs to kanban cards.
 */

const FORGEJO_URL = Deno.env.get("FORGEJO_URL") ?? "https://src.sunbeam.pt";
const FORGEJO_TOKEN = Deno.env.get("FORGEJO_TOKEN") ?? "";

interface ForgejoIssue {
  number: number;
  title: string;
  state: string;
  html_url: string;
  pull_request?: { merged: boolean } | null;
  repository?: { full_name: string };
}

export interface ForgejoResult {
  type: string;
  repo: string;
  number: number;
  url: string;
  title: string;
  state: string;
}

/**
 * Search Forgejo issues and pull requests.
 */
export async function searchForgejoIssues(
  query: string,
  repo?: string,
): Promise<ForgejoResult[]> {
  const params = new URLSearchParams({
    q: query,
    limit: "20",
    type: "issues",
  });

  let apiUrl: string;
  if (repo) {
    // Search within a specific repo
    apiUrl = `${FORGEJO_URL}/api/v1/repos/${repo}/issues?${params}`;
  } else {
    // Global search
    apiUrl = `${FORGEJO_URL}/api/v1/repos/search?q=${encodeURIComponent(query)}&limit=5`;
    // Forgejo doesn't have a global issue search endpoint that's easy to use,
    // so we search repos first, then get issues from top repos
    try {
      const headers: Record<string, string> = { "Accept": "application/json" };
      if (FORGEJO_TOKEN) headers["Authorization"] = `token ${FORGEJO_TOKEN}`;

      const repoResp = await fetch(apiUrl, { headers });
      if (!repoResp.ok) return [];
      const repos = await repoResp.json();
      const repoData = repos?.data ?? repos;
      if (!Array.isArray(repoData)) return [];

      const results: ForgejoResult[] = [];
      for (const r of repoData.slice(0, 3)) {
        const fullName = r.full_name;
        const issueUrl = `${FORGEJO_URL}/api/v1/repos/${fullName}/issues?${params}`;
        const issueResp = await fetch(issueUrl, { headers });
        if (!issueResp.ok) continue;
        const issues: ForgejoIssue[] = await issueResp.json();
        for (const issue of issues) {
          const isPr = issue.pull_request != null;
          let state = issue.state;
          if (isPr && issue.pull_request?.merged) state = "merged";
          results.push({
            type: isPr ? "pr" : "issue",
            repo: fullName,
            number: issue.number,
            url: issue.html_url,
            title: issue.title,
            state,
          });
        }
      }
      return results;
    } catch (err) {
      console.error("Forgejo search error:", err);
      return [];
    }
  }

  // Repo-specific search
  try {
    const headers: Record<string, string> = { "Accept": "application/json" };
    if (FORGEJO_TOKEN) headers["Authorization"] = `token ${FORGEJO_TOKEN}`;

    const resp = await fetch(apiUrl, { headers });
    if (!resp.ok) return [];
    const issues: ForgejoIssue[] = await resp.json();

    return issues.map((issue) => {
      const isPr = issue.pull_request != null;
      let state = issue.state;
      if (isPr && issue.pull_request?.merged) state = "merged";
      return {
        type: isPr ? "pr" : "issue",
        repo: repo!,
        number: issue.number,
        url: issue.html_url,
        title: issue.title,
        state,
      };
    });
  } catch (err) {
    console.error("Forgejo search error:", err);
    return [];
  }
}
