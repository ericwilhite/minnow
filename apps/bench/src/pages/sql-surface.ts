import sqlFeatureMatrix from "@browserdatabase/engine/sql-feature-matrix.json";
import "../style.css";
import { escapeHtml, required } from "../ui/format.js";
import { highlightAll } from "../ui/highlight.js";
import { initShell } from "../ui/shell.js";

interface SqlFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  error?: string;
  notes?: string;
}

initShell();

const features = (sqlFeatureMatrix as { features: SqlFeature[] }).features;
const supported = features.filter((feature) => feature.status === "supported");
const unsupported = features.filter((feature) => feature.status !== "supported");

required("#sql-summary").textContent =
  `${String(supported.length)} of ${String(features.length)} cataloged SQL features are supported; ${String(unsupported.length)} are explicitly not.`;
required("#sql-supported").innerHTML = supported.map(featureCard).join("");
required("#sql-unsupported").innerHTML = unsupported.map(featureCard).join("");
void highlightAll();

function featureCard(feature: SqlFeature): string {
  const error =
    feature.status === "supported" || feature.error === undefined
      ? ""
      : `<em>${escapeHtml(feature.error)}</em>`;
  // A note distinguishes a missing capability from one reachable through a typed API, or
  // from a deliberate restriction; without it the list reads as a flat catalog of gaps.
  const note = feature.notes === undefined ? "" : `<small>${escapeHtml(feature.notes)}</small>`;
  return `<article class="sql-feature ${feature.status}">
    <strong>${escapeHtml(feature.id)}</strong>
    <pre><code data-lang="sql">${escapeHtml(feature.example)}</code></pre>
    ${error}
    ${note}
  </article>`;
}
