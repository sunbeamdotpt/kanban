/**
 * LiveReact — the right pane of the /__preview visual diff.
 *
 * Renders the reference body markup verbatim (parsed via DOMParser) with
 * the reference CSS injected as a scoped style block. This makes the
 * right pane visually identical to /__ref-page.html on the left.
 *
 * Iteration plan: extract sections of ref-body.html into real React /
 * beam-ui components one block at a time. Replace the static block in
 * ref-body.html with a placeholder marker like
 *   <div data-react-slot="topbar"></div>
 * and mount the React component into the matching slot via portal.
 * When the React render matches the reference visually, that section is
 * "ported" and we move on to the next.
 *
 * Both ref-styles.css and ref-body.html are extracted from the reference
 * page via the playwright dump (see public/__ref-page.html for source).
 */

import { useEffect, useRef } from "react";
import refCss from "./ref-styles.css?raw";
import refBody from "./ref-body.html?raw";

export function LiveReact() {
  const hostRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    while (host.firstChild) host.removeChild(host.firstChild);
    const range = document.createRange();
    range.selectNodeContents(host);
    const frag = range.createContextualFragment(refBody);
    host.appendChild(frag);
  }, []);

  return (
    <>
      <style>{refCss}</style>
      <div ref={hostRef} />
    </>
  );
}
