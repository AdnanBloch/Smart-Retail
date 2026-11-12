"use strict";
// Review explorer: one page over the data that build.py extracts from docs/ and the code.
// Routes live in the hash (#/decision/ORD-D1), so every view is a link that can be shared.

const D = window.REVIEW_DATA;
if (!D) {
  document.querySelector(".layout").hidden = true;
  document.getElementById("missing").hidden = false;
  throw new Error("data.js missing: run tools/review_explorer/build.py");
}

const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const plainText = (s) => String(s ?? "").replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1").replace(/[`*]/g, "").trim();
const truncate = (s, n) => (s.length > n ? s.slice(0, n - 1).trimEnd() + "…" : s);
const ID_RE = /\b((?:ARC|PLT|GW|IDN|CAT|ORD|INV|NTF|ANL|AST)-D\d+)\b|\b(F[1-9]\d?)\b/g;
const HAS_ID = new RegExp(ID_RE.source); // no /g, so .test() keeps no state between calls
const unlink = (s) => String(s ?? "").replace(/(?<!!)\[([^\]]*)\]\([^)]*\)/g, "$1");

// --- Indexes -----------------------------------------------------------------

const docsById = new Map(D.docs.map((d) => [d.id, d]));
const docsByPath = new Map(D.docs.map((d) => [d.path, d]));
const decById = new Map(D.decisions.map((d) => [d.id, d]));
const failById = new Map(D.failures.map((f) => [f.id, f]));
const svcByPrefix = new Map(D.services.map((s) => [s.prefix, s]));
const ruleByNum = new Map(D.rules.map((r) => [r.num, r]));
const rulesDoc = docsById.get("business-logic-and-decisions");
const flowsDoc = docsById.get("business-flows");
const failuresDoc = docsById.get("distributed-systems-and-failure-modes");
const reviewsDoc = docsById.get("mentor-reviews");

const multimap = () => new Map();
const push = (map, key, value) => (map.get(key) || map.set(key, []).get(key)).push(value);
const rulesByDec = multimap();
D.rules.forEach((r) => r.ids.forEach((id) => push(rulesByDec, id, r)));
const failsByDec = multimap();
D.failures.forEach((f) => f.ids.forEach((id) => push(failsByDec, id, f)));
const mentions = multimap(); // decision or failure ID -> sections that name it
for (const doc of D.docs) {
  if (doc.group === "Archived RFCs") continue;
  for (const s of doc.sections) for (const id of [...s.ids, ...s.failures]) push(mentions, id, { doc, s });
}

const lines = (doc) => (doc._lines ||= doc.md.split("\n"));
function sectionMd(doc, n, withChildren = true) {
  const s = doc.sections[n];
  let end = s.end;
  if (withChildren) {
    const next = doc.sections.slice(n + 1).find((x) => x.level <= s.level);
    end = next ? next.start - 1 : lines(doc).length;
  }
  return lines(doc).slice(s.start - 1, end).join("\n");
}
const idsIn = (text) => {
  const ids = new Set(), fails = new Set();
  for (const m of text.matchAll(ID_RE)) {
    if (m[1] && decById.has(m[1])) ids.add(m[1]);
    if (m[2] && failById.has(m[2])) fails.add(m[2]);
  }
  return { ids: [...ids], fails: [...fails] };
};

// --- State --------------------------------------------------------------------

const state = {
  lens: localStorage.getItem("re.lens") || "technical",
  theme: localStorage.getItem("re.theme") || "light",
  seq: null, // {kind, keys, label}: the list a detail page pages through
};
function setLens(lens) {
  state.lens = lens;
  localStorage.setItem("re.lens", lens);
  $$(".lens button").forEach((b) => b.classList.toggle("on", b.dataset.lens === lens));
  route();
}
function setTheme(theme) {
  state.theme = theme;
  localStorage.setItem("re.theme", theme);
  document.documentElement.dataset.theme = theme;
  mermaid.initialize({ startOnLoad: false, theme: theme === "dark" ? "dark" : "default", securityLevel: "strict" });
}

// --- Markdown -------------------------------------------------------------------

// marked and mermaid come from a CDN. Offline, text still shows, unformatted, and diagrams stay as source.
if (!window.marked) window.marked = { setOptions() {}, parse: (t) => `<pre>${esc(t)}</pre>`, parseInline: esc };
if (!window.mermaid) window.mermaid = { initialize() {}, run: () => Promise.resolve() };
marked.setOptions({ gfm: true });
const md = (text) => marked.parse(text || "");
const inline = (text) => marked.parseInline(text || "");
const vscode = (path, line) => `vscode://file/${D.root}/${path}${line ? ":" + line : ""}`;

function slugger() {
  const seen = new Map();
  return (text) => {
    let slug = text.trim().toLowerCase().replace(/[^\p{L}\p{N}\s_-]/gu, "").replace(/\s/g, "-");
    const n = seen.get(slug) || 0;
    seen.set(slug, n + 1);
    return n ? `${slug}-${n}` : slug;
  };
}

function normalizePath(path) {
  const out = [];
  for (const part of path.split("/")) {
    if (part === "..") out.pop();
    else if (part && part !== ".") out.push(part);
  }
  return out.join("/");
}

function resolveHref(href, ctx) {
  if (/^(https?:|mailto:|vscode:)/.test(href)) return { href, external: true };
  if (href.startsWith("#/")) return { href };
  if (href.startsWith("#")) return ctx ? { href: `#/doc/${ctx.id}/${href.slice(1)}` } : { href };
  const [rawPath, anchor = ""] = href.split("#");
  const base = ctx ? ctx.path.split("/").slice(0, -1).join("/") : "";
  const path = normalizePath(`${base}/${decodeURIComponent(rawPath)}`);
  const doc = docsByPath.get(path);
  if (doc) return { href: `#/doc/${doc.id}/${anchor}` };
  return { href: vscode(path), external: true };
}

function tooltip(id) {
  const dec = decById.get(id);
  if (dec) return `${id}: ${truncate(plainText(dec.decision), 220)}`;
  const f = failById.get(id);
  return f ? `${id}: ${plainText(f.scenario)}` : id;
}

// Turn decision and failure IDs in plain text into links, outside links, code blocks and diagrams.
function linkify(root) {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
    acceptNode: (node) =>
      node.parentElement.closest("a, pre, .mermaid, script, h1") ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT,
  });
  const nodes = [];
  while (walker.nextNode()) if (HAS_ID.test(walker.currentNode.nodeValue)) nodes.push(walker.currentNode);
  for (const node of nodes) {
    const text = node.nodeValue;
    const frag = document.createDocumentFragment();
    let last = 0;
    for (const m of text.matchAll(ID_RE)) {
      const id = m[1] || m[2];
      const known = m[1] ? decById.has(id) : failById.has(id);
      if (!known) continue;
      frag.append(text.slice(last, m.index));
      const a = document.createElement("a");
      a.className = "id";
      a.href = m[1] ? `#/decision/${id}` : `#/failure/${id}`;
      a.title = tooltip(id);
      a.textContent = id;
      frag.append(a);
      last = m.index + id.length;
    }
    frag.append(text.slice(last));
    node.replaceWith(frag);
  }
}

// Everything rendered from Markdown passes through here: heading anchors, links, IDs, diagrams.
function enhance(root, ctx) {
  const slug = slugger();
  $$("h1, h2, h3, h4, h5, h6", root).forEach((h, i) => {
    if (!h.id) h.id = slug(h.textContent);
    h.dataset.n = i;
  });
  $$("a[href]", root).forEach((a) => {
    const { href, external } = resolveHref(a.getAttribute("href"), ctx);
    a.setAttribute("href", href);
    if (external && !href.startsWith("vscode:")) a.target = "_blank";
  });
  linkify(root);
  const diagrams = $$("pre > code.language-mermaid", root).map((code) => {
    const div = document.createElement("div");
    div.className = "mermaid";
    div.textContent = code.textContent;
    code.parentElement.replaceWith(div);
    return div;
  });
  return diagrams.length ? mermaid.run({ nodes: diagrams, suppressErrors: true }).catch(() => {}) : Promise.resolve();
}

function splitAlternatives(text) {
  const parts = [];
  let depth = 0, tick = false, buf = "";
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === "`") tick = !tick;
    else if (!tick && "([".includes(c)) depth++;
    else if (!tick && ")]".includes(c)) depth = Math.max(0, depth - 1);
    if (c === ";" && depth === 0 && !tick) {
      parts.push(buf.trim());
      buf = "";
    } else buf += c;
  }
  parts.push(buf.trim());
  return parts.filter(Boolean);
}
const altList = (text) => {
  const items = splitAlternatives(text);
  return items.length > 1 ? `<ul class="alts">${items.map((a) => `<li>${inline(a)}</li>`).join("")}</ul>` : inline(text);
};

// --- Shared fragments --------------------------------------------------------------

const svcTag = (prefix) => `<span class="tag tech">${esc(svcByPrefix.get(prefix)?.name || prefix)}</span>`;
const statusTag = (status) =>
  status ? `<span class="tag ${status}">${{ implemented: "✅ built", deviation: "🟡 deviation", designed: "⏳ designed" }[status]}</span>` : "";
const decLink = (id) => `<a class="id" href="#/decision/${id}" title="${esc(tooltip(id))}">${id}</a>`;
const failLink = (id) => `<a class="id" href="#/failure/${id}" title="${esc(tooltip(id))}">${id}</a>`;
const sectionHref = (doc, n) => `#/doc/${doc.id}/@${n}`;

function snippet(ref, opts = {}) {
  const code = ref.code || "";
  const body = code
    .split("\n")
    .map((l, i) => {
      const n = ref.from + i;
      return `<span class="ln${n === ref.line && !opts.noHit ? " hit" : ""}" data-n="${n}">${esc(l) || " "}</span>`;
    })
    .join("");
  return `<div class="snippet">
    <header><span class="path">${esc(ref.file)}:${ref.line}</span>${ref.test ? '<span class="tag">test</span>' : ""}
      <span class="spacer"></span><a href="${vscode(ref.file, ref.line)}">Open in VS Code</a></header>
    ${code ? `<pre>${body}</pre>` : ""}</div>`;
}

function pager(kind, key) {
  const seq = state.seq && state.seq.kind === kind && state.seq.keys.includes(key) ? state.seq : defaultSeq(kind, key);
  if (!seq) return "";
  const i = seq.keys.indexOf(key);
  const prev = seq.keys[i - 1], next = seq.keys[i + 1];
  pager.current = { prev: prev && hrefFor(kind, prev), next: next && hrefFor(kind, next) };
  return `<div class="pager">
    ${prev ? `<a class="btn" href="${hrefFor(kind, prev)}">← ${esc(String(prev))}</a>` : ""}
    <span>${i + 1} / ${seq.keys.length} ${esc(seq.label || "")}</span>
    ${next ? `<a class="btn" href="${hrefFor(kind, next)}">${esc(String(next))} →</a>` : ""}
    <span class="small">keys <kbd>←</kbd> <kbd>→</kbd></span></div>`;
}
const hrefFor = (kind, key) => `#/${kind}/${encodeURIComponent(key)}`;
function defaultSeq(kind, key) {
  if (kind === "decision") {
    const prefix = key.split("-")[0];
    return { kind, keys: D.decisions.filter((d) => d.prefix === prefix).map((d) => d.id), label: `in ${svcByPrefix.get(prefix)?.name}` };
  }
  if (kind === "rule") return { kind, keys: D.rules.map((r) => r.num), label: "business rules" };
  if (kind === "failure") return { kind, keys: D.failures.map((f) => f.id), label: "failure cases" };
  if (kind === "flow") return { kind, keys: D.flows.map((_, i) => String(i)), label: "flows" };
  if (kind === "review") return { kind, keys: D.reviews.map((_, i) => String(i)), label: "review points" };
  return null;
}

function tabs(id, panels, initial) {
  const visible = panels.filter((p) => p.html !== null);
  const start = visible.find((p) => p.key === initial) ? initial : visible[0]?.key;
  return `<div class="tabs" data-tabs="${id}">${visible
    .map((p) => `<button data-tab="${p.key}" class="${p.key === start ? "on" : ""}">${esc(p.label)}${p.count != null ? ` <span class="count">${p.count}</span>` : ""}</button>`)
    .join("")}</div>${visible
    .map((p) => `<div class="tabpanel" data-panel="${p.key}" ${p.key === start ? "" : "hidden"}>${p.html || '<p class="muted">Nothing recorded.</p>'}</div>`)
    .join("")}`;
}
document.addEventListener("click", (e) => {
  const btn = e.target.closest("[data-tab]");
  if (!btn) return;
  const group = btn.parentElement;
  $$("button", group).forEach((b) => b.classList.toggle("on", b === btn));
  let el = group.nextElementSibling;
  while (el && el.classList.contains("tabpanel")) {
    el.hidden = el.dataset.panel !== btn.dataset.tab;
    el = el.nextElementSibling;
  }
});

function mentionList(id, exclude) {
  const found = (mentions.get(id) || []).filter((m) => !(exclude && m.doc.id === exclude.doc && m.s.n === exclude.section));
  if (!found.length) return { html: null, count: 0 };
  const order = { Guides: 0, "Mentor reviews": 1, RFCs: 2, "Specification (as supplied)": 3 };
  const byDoc = multimap();
  found.forEach((m) => push(byDoc, m.doc, m.s));
  const html = [...byDoc]
    .sort((a, b) => (order[a[0].group] ?? 9) - (order[b[0].group] ?? 9))
    .map(([doc, secs]) => `<div class="card"><div class="row"><b>${esc(doc.title)}</b><span class="tag">${esc(doc.group)}</span></div>
      <ul>${secs.map((s) => `<li><a href="${sectionHref(doc, s.n)}">${esc(s.title)}</a></li>`).join("")}</ul></div>`)
    .join("");
  return { html, count: found.length };
}

// --- Views ---------------------------------------------------------------------------

const SUGGESTIONS = [
  "Why Temporal for checkout?",
  "What if the card is declined?",
  "How is overselling prevented?",
  "Why does checkout return 202?",
  "What happens if Kafka is down?",
  "Why no cart?",
  "double click buy",
  "Why Kafka and RabbitMQ and Temporal?",
  "How are refunds handled?",
  "rate limiting",
];

function viewHome() {
  const biz = state.lens === "business";
  const linked = D.decisions.filter((d) => D.codeRefs[d.id]).length;
  const archDoc = docsById.get("architecture-diagrams");
  const arch = archDoc && (archDoc.md.match(/```mermaid\n[\s\S]*?```/) || [""])[0];
  const stats = [
    ["Decisions", D.decisions.length, "#/decisions"],
    ["Business rules", D.rules.length, "#/rules"],
    ["Business flows", D.flows.length, "#/flows"],
    ["Failure cases", D.failures.length, "#/failures"],
    ["Review points", D.reviews.length, "#/reviews"],
    ["Decisions traced to code", linked, "#/decisions?has=code"],
  ];
  const dates = [...new Set(D.reviews.map((r) => r.date))];
  return `
    <h1>SmartRetail design review</h1>
    <p class="lede">Every decision with its reason, the alternatives rejected, the business rule it serves and the code and tests that implement it.
      Built from <code>docs/</code> and the code at <code>${esc(D.git.commit)}</code> on <code>${esc(D.git.branch)}</code>, ${esc(D.built)}.</p>
    <div class="card answer">
      <form class="ask" data-home-ask><input type="search" placeholder="Ask a question in plain words…" autofocus></form>
      <div class="filters">${SUGGESTIONS.map((s) => `<button class="chip" data-ask="${esc(s)}">${esc(s)}</button>`).join("")}</div>
    </div>
    <div class="grid">${stats.map(([label, n, href]) => `<a class="card" href="${href}"><div class="stat">${n}</div><div class="muted">${label}</div></a>`).join("")}</div>
    ${biz ? homeBusiness() : homeTechnical(arch)}
    <h2>Mentor reviews</h2>
    <div class="grid">${dates
      .map((d) => {
        const i = D.reviews.findIndex((r) => r.date === d);
        const pts = D.reviews.filter((r) => r.date === d && r.title !== "Overview");
        return `<a class="card" href="#/review/${i}"><b>${d}</b><div class="small muted">${pts.map((p) => esc(p.title)).join(" · ")}</div></a>`;
      })
      .join("")}</div>`;
}

function homeBusiness() {
  return `
    <h2>Business flows</h2>
    <div class="grid">${D.flows
      .map((f, i) => {
        const { ids, fails } = idsIn(sectionMd(flowsDoc, f.section));
        return `<a class="card biz-card" href="#/flow/${i}"><b>${esc(f.title)}</b><div class="small muted">${ids.length} decisions · ${fails.length} failure cases</div></a>`;
      })
      .join("")}</div>
    <h2>Where the requirements were silent</h2>
    <p class="lede">The brief gave no answer on these. Each row is our choice and the strongest alternative, so a reviewer can challenge it.</p>
    <div class="card"><table><thead><tr><th>Topic</th><th>Our decision</th><th>Strongest alternative</th></tr></thead><tbody>
      ${D.silent.map((s) => `<tr><td>${inline(s.topic)}</td><td>${inline(s.decision)}</td><td>${inline(s.alternative)}</td></tr>`).join("")}
    </tbody></table></div>
    <h2>The specification</h2>
    <div class="grid">${D.docs
      .filter((d) => d.group === "Specification (as supplied)")
      .map((d) => `<a class="card" href="#/doc/${d.id}/"><b>${esc(d.title)}</b><div class="small muted">${esc(d.path)}</div></a>`)
      .join("")}</div>`;
}

function homeTechnical(arch) {
  return `
    <h2>Architecture</h2>
    <div class="card prose" data-md>${md(arch || "")}<a href="#/doc/architecture-diagrams/">All diagrams →</a></div>
    <h2>Services</h2>
    <div class="grid">${D.services
      .map((s) => {
        const coded = D.decisions.filter((d) => d.prefix === s.prefix && D.codeRefs[d.id]).length;
        return `<a class="card tech-card" href="#/decisions?svc=${s.prefix}"><b>${esc(s.name)}</b> <span class="muted small">${s.prefix}</span>
          <div class="small muted">${s.count} decisions · ${coded} traced to code</div></a>`;
      })
      .join("")}</div>
    <h2>Live stack</h2>
    <p class="lede">These work while <code>make up</code> is running.</p>
    <div class="grid">${D.live.map((l) => `<a class="card" href="${l.url}" target="_blank"><b>${esc(l.name)}</b><div class="small muted">${esc(l.what)}</div><div class="small">${esc(l.url)}</div></a>`).join("")}</div>`;
}

// Decisions -------------------------------------------------------------------------

function viewDecisions(q) {
  const svc = q.get("svc") || "";
  const text = q.get("q") || "";
  const has = q.get("has") || "";
  return `
    <h1>Decisions</h1>
    <p class="lede">The decision register of every RFC: what was chosen, why, what was rejected and when to revisit it.</p>
    <div class="filters">
      <button class="chip ${svc ? "" : "on"}" data-filter="svc" data-value="">All ${D.decisions.length}</button>
      ${D.services.map((s) => `<button class="chip ${svc === s.prefix ? "on" : ""}" data-filter="svc" data-value="${s.prefix}">${esc(s.name)} ${s.count}</button>`).join("")}
    </div>
    <div class="filters">
      <input data-filter-text placeholder="Filter by words or ID…" value="${esc(text)}">
      <button class="chip ${has === "code" ? "on" : ""}" data-filter="has" data-value="${has === "code" ? "" : "code"}">Traced to code</button>
      <button class="chip ${has === "rule" ? "on" : ""}" data-filter="has" data-value="${has === "rule" ? "" : "rule"}">Has a business rule</button>
    </div>
    <ul class="list" data-results></ul>`;
}
function fillDecisions(q) {
  const svc = q.get("svc") || "", has = q.get("has") || "";
  const terms = (q.get("q") || "").toLowerCase().split(/\s+/).filter(Boolean);
  const list = D.decisions.filter((d) => {
    if (svc && d.prefix !== svc) return false;
    if (has === "code" && !D.codeRefs[d.id]) return false;
    if (has === "rule" && !rulesByDec.has(d.id)) return false;
    const hay = `${d.id} ${d.decision} ${d.why} ${d.alternatives} ${d.legacy.join(" ")}`.toLowerCase();
    return terms.every((t) => hay.includes(t));
  });
  state.seq = { kind: "decision", keys: list.map((d) => d.id), label: svc ? `in ${svcByPrefix.get(svc).name}` : "decisions" };
  return list
    .map((d) => {
      const refs = D.codeRefs[d.id]?.length || 0, rules = rulesByDec.get(d.id)?.length || 0, fails = failsByDec.get(d.id)?.length || 0;
      return `<li><div class="row">${decLink(d.id)} ${svcTag(d.prefix)}
        ${rules ? `<span class="tag biz">${rules} rule${rules > 1 ? "s" : ""}</span>` : ""}
        ${refs ? `<span class="tag tech">${refs} code ref${refs > 1 ? "s" : ""}</span>` : ""}
        ${fails ? `<span class="tag">${fails} failure case${fails > 1 ? "s" : ""}</span>` : ""}</div>
        <div><a href="#/decision/${d.id}" class="title" style="color:inherit">${inline(unlink(d.decision))}</a></div>
        <div class="small muted">Why: ${esc(truncate(plainText(d.why), 240))}</div></li>`;
    })
    .join("") || '<li class="muted">No decision matches.</li>';
}

function viewDecision(id) {
  const d = decById.get(id);
  if (!d) return notFound(`Decision ${id}`);
  const doc = docsById.get(d.doc);
  const svc = svcByPrefix.get(d.prefix);
  const rules = rulesByDec.get(id) || [];
  const fails = failsByDec.get(id) || [];
  const refs = D.codeRefs[id] || [];
  const code = refs.filter((r) => !r.test), tests = refs.filter((r) => r.test);
  const discussed = mentionList(id, { doc: d.doc, section: d.section });
  const biz = state.lens === "business";

  const bizHtml = rules.length
    ? rules.map((r) => ruleCard(r, true)).join("")
    : null;
  const codeHtml = refs.length
    ? `${code.length ? `<h3>Implementation</h3>${code.map((r) => snippet(r)).join("")}` : ""}
       ${tests.length ? `<h3>Tests that name it</h3>${tests.map((r) => snippet(r)).join("")}` : ""}`
    : null;
  const failHtml = fails.length ? fails.map((f) => failureCard(f)).join("") : null;
  const initial = biz ? (rules.length ? "biz" : "discussed") : refs.length ? "code" : fails.length ? "fails" : "discussed";

  return `
    <div class="row"><div class="crumbs"><a href="#/decisions">Decisions</a> › <a href="#/decisions?svc=${d.prefix}">${esc(svc?.name)}</a></div>
      <span class="spacer"></span>${pager("decision", id)}</div>
    <div class="row"><h1>${id}</h1>${svcTag(d.prefix)}
      ${d.legacy.length ? `<span class="small muted">also known as ${d.legacy.map(esc).join(", ")}</span>` : ""}</div>
    <div class="card chosen" data-md data-ctx="${d.doc}">
      <dl class="fields">
        <dt>Decision</dt><dd>${inline(d.decision)}</dd>
        <dt>Why</dt><dd>${inline(d.why)}</dd>
        <dt>Alternatives rejected</dt><dd>${altList(d.alternatives)}</dd>
        <dt>Revisit if</dt><dd>${inline(d.revisit)}</dd>
      </dl>
      <div class="row small" style="margin-top:10px">
        <a href="${sectionHref(doc, d.section)}">Open in ${esc(doc.title)}</a> ·
        <a href="${vscode(doc.path, d.line)}">${esc(doc.path)}:${d.line}</a></div>
    </div>
    ${tabs("decision", [
      { key: "biz", label: "Business side", count: rules.length, html: bizHtml },
      { key: "code", label: "Code path", count: refs.length, html: codeHtml },
      { key: "fails", label: "Failure cases", count: fails.length, html: failHtml },
      { key: "discussed", label: "Discussed in", count: discussed.count, html: discussed.html },
    ], initial)}`;
}

// Business rules ------------------------------------------------------------------------

function ruleCard(r, compact) {
  return `<div class="card biz-card" data-md data-ctx="business-logic-and-decisions">
    <div class="row"><a href="#/rule/${encodeURIComponent(r.num)}"><b>Rule ${esc(r.num)}</b></a><span class="tag biz">${esc(r.area)}</span>
      ${statusTag(r.status)}${r.silent ? '<span class="tag silent">requirement silent</span>' : ""}</div>
    <dl class="fields" style="margin-top:8px">
      <dt>Rule</dt><dd>${inline(r.rule)}</dd>
      <dt>Why</dt><dd>${inline(r.why)}</dd>
      <dt>Requirement</dt><dd>${inline(r.source)}</dd>
      ${compact ? "" : `<dt>Alternatives rejected</dt><dd>${altList(r.alternatives)}</dd>`}
      <dt>Decisions</dt><dd>${r.ids.length ? r.ids.map(decLink).join(", ") : esc(plainText(r.ids_raw)) || "—"}</dd>
    </dl></div>`;
}

function viewRules(q) {
  const area = q.get("area") || "", text = q.get("q") || "", only = q.get("only") || "";
  const areas = [...new Set(D.rules.map((r) => r.area))];
  return `
    <h1>Business rules</h1>
    <p class="lede">Each rule the system enforces, the requirement it comes from (or <b>Silent</b> when the brief says nothing), why we chose it and what we rejected.</p>
    <div class="filters">
      <button class="chip ${area ? "" : "on"}" data-filter="area" data-value="">All ${D.rules.length}</button>
      ${areas.map((a) => `<button class="chip ${area === a ? "on" : ""}" data-filter="area" data-value="${esc(a)}">${esc(a.replace(/^\d+\.\s*/, ""))}</button>`).join("")}
    </div>
    <div class="filters"><input data-filter-text placeholder="Filter rules…" value="${esc(text)}">
      ${[["silent", "Requirement silent"], ["designed", "⏳ Not built yet"], ["deviation", "🟡 Deviations"]]
        .map(([v, l]) => `<button class="chip ${only === v ? "on" : ""}" data-filter="only" data-value="${only === v ? "" : v}">${l}</button>`)
        .join("")}</div>
    <ul class="list" data-results></ul>`;
}
function fillRules(q) {
  const area = q.get("area") || "", only = q.get("only") || "";
  const terms = (q.get("q") || "").toLowerCase().split(/\s+/).filter(Boolean);
  const list = D.rules.filter((r) => {
    if (area && r.area !== area) return false;
    if (only === "silent" && !r.silent) return false;
    if ((only === "designed" || only === "deviation") && r.status !== only) return false;
    const hay = `${r.num} ${r.rule} ${r.why} ${r.source} ${r.alternatives} ${r.ids_raw}`.toLowerCase();
    return terms.every((t) => hay.includes(t));
  });
  state.seq = { kind: "rule", keys: list.map((r) => r.num), label: "rules" };
  return list
    .map((r) => `<li><div class="row"><a href="#/rule/${encodeURIComponent(r.num)}"><b>${esc(r.num)}</b></a><span class="tag biz">${esc(r.area)}</span>${statusTag(r.status)}
        ${r.silent ? '<span class="tag silent">silent</span>' : ""} ${r.ids.map(decLink).join(" ")}</div>
        <div>${inline(unlink(r.rule))}</div><div class="small muted">Why: ${esc(truncate(plainText(r.why), 220))}</div></li>`)
    .join("") || '<li class="muted">No rule matches.</li>';
}

function viewRule(num) {
  const r = ruleByNum.get(num);
  if (!r) return notFound(`Rule ${num}`);
  const decs = r.ids.map((id) => decById.get(id));
  const fails = [...new Set(r.ids.flatMap((id) => failsByDec.get(id) || []))];
  const refs = r.ids.flatMap((id) => (D.codeRefs[id] || []).filter((x) => !x.test).slice(0, 3));
  return `
    <div class="row"><div class="crumbs"><a href="#/rules">Business rules</a> › ${esc(r.area)}</div><span class="spacer"></span>${pager("rule", num)}</div>
    <h1>Rule ${esc(r.num)}</h1>
    ${ruleCard(r, false)}
    <p class="small"><a href="${sectionHref(rulesDoc, r.section)}">Open the rule in context</a></p>
    ${tabs("rule", [
      { key: "tech", label: "Technical side", count: decs.length, html: decs.length ? decs.map(decisionCard).join("") : null },
      { key: "code", label: "Code path", count: refs.length, html: refs.length ? refs.map((x) => snippet(x)).join("") : null },
      { key: "fails", label: "Failure cases", count: fails.length, html: fails.length ? fails.map(failureCard).join("") : null },
    ], state.lens === "business" ? "tech" : refs.length ? "code" : "tech")}`;
}

function decisionCard(d) {
  return `<div class="card tech-card" data-md data-ctx="${d.doc}">
    <div class="row">${decLink(d.id)} ${svcTag(d.prefix)}</div>
    <dl class="fields" style="margin-top:8px">
      <dt>Decision</dt><dd>${inline(d.decision)}</dd>
      <dt>Why</dt><dd>${inline(d.why)}</dd>
      <dt>Rejected</dt><dd>${altList(d.alternatives)}</dd>
    </dl></div>`;
}

// Failures ------------------------------------------------------------------------------

function failureCard(f) {
  return `<div class="card" data-md data-ctx="distributed-systems-and-failure-modes">
    <div class="row">${failLink(f.id)} <b>${inline(unlink(f.scenario))}</b></div>
    <dl class="fields" style="margin-top:8px"><dt>What happens</dt><dd>${inline(f.outcome)}</dd><dt>Mechanism</dt><dd>${inline(f.mechanism)}</dd></dl></div>`;
}

function viewFailures(q) {
  const text = q.get("q") || "";
  return `<h1>Failure cases</h1>
    <p class="lede">What happens when something goes wrong, the mechanism that handles it, and the test that proves it.</p>
    <div class="filters"><input data-filter-text placeholder="Filter: kafka, crash, payment…" value="${esc(text)}"></div>
    <ul class="list" data-results></ul>`;
}
function fillFailures(q) {
  const terms = (q.get("q") || "").toLowerCase().split(/\s+/).filter(Boolean);
  const list = D.failures.filter((f) => terms.every((t) => `${f.id} ${f.scenario} ${f.outcome} ${f.mechanism} ${f.proof}`.toLowerCase().includes(t)));
  state.seq = { kind: "failure", keys: list.map((f) => f.id), label: "failure cases" };
  return list
    .map((f) => `<li><div class="row">${failLink(f.id)} <a class="title" style="color:inherit" href="#/failure/${f.id}">${inline(unlink(f.scenario))}</a></div>
      <div class="small muted">${esc(truncate(plainText(f.outcome), 260))}</div></li>`)
    .join("") || '<li class="muted">No failure case matches.</li>';
}

function viewFailure(id) {
  const f = failById.get(id);
  if (!f) return notFound(`Failure case ${id}`);
  const found = f.tests.flatMap((t) => t.found.filter((x) => !x.module).map((x) => ({ ...x, test: true })));
  const modules = f.tests.flatMap((t) => t.found.filter((x) => x.module));
  const missing = f.tests.filter((t) => !t.found.length).map((t) => t.name);
  return `
    <div class="row"><div class="crumbs"><a href="#/failures">Failure cases</a></div><span class="spacer"></span>${pager("failure", id)}</div>
    <h1>${id}: ${esc(plainText(f.scenario))}</h1>
    <div class="card chosen" data-md data-ctx="distributed-systems-and-failure-modes"><dl class="fields">
      <dt>What happens</dt><dd>${inline(f.outcome)}</dd>
      <dt>Mechanism</dt><dd>${inline(f.mechanism)}</dd>
      <dt>Proven by</dt><dd>${inline(f.proof)}</dd></dl>
      <p class="small"><a href="${sectionHref(failuresDoc, f.section)}">Open the failure catalogue</a></p></div>
    ${tabs("failure", [
      { key: "tests", label: "Tests", count: found.length, html: found.length || modules.length
          ? found.map((x) => snippet(x, { noHit: true })).join("") + modules.map((x) => snippet(x)).join("") +
            (missing.length ? `<p class="muted small">Not found in code: ${missing.map(esc).join(", ")}</p>` : "")
          : null },
      { key: "decisions", label: "Decisions", count: f.ids.length, html: f.ids.length ? f.ids.map((x) => decisionCard(decById.get(x))).join("") : null },
      { key: "discussed", label: "Discussed in", ...mentionList(id) },
    ], state.lens === "business" ? "decisions" : "tests")}`;
}

// Flows -------------------------------------------------------------------------------

function viewFlows() {
  state.seq = null;
  return `<h1>Business flows</h1>
    <p class="lede">Every flow end to end, with its failure and compensation paths. Each one links to the decisions, rules and failure cases it relies on.</p>
    <div class="grid">${D.flows
      .map((f, i) => {
        const text = sectionMd(flowsDoc, f.section);
        const { ids, fails } = idsIn(text);
        const intro = plainText((text.split("\n").slice(1).find((l) => l.trim() && !l.startsWith("```") && !l.startsWith("|")) || ""));
        return `<a class="card biz-card" href="#/flow/${i}"><b>${esc(f.title)}</b><div class="small muted">${esc(truncate(intro, 160))}</div>
          <div class="small">${ids.length} decisions · ${fails.length} failure cases</div></a>`;
      })
      .join("")}</div>
    <p><a href="#/doc/business-flows/">Read the whole page</a> · <a href="#/doc/architecture-diagrams/">Architecture diagrams</a></p>`;
}

function relatedPanel(text, extraIds = []) {
  const { ids, fails } = idsIn(text);
  const allIds = [...new Set([...ids, ...extraIds])];
  const failSet = new Set([...fails, ...allIds.flatMap((id) => (failsByDec.get(id) || []).map((f) => f.id))]);
  const rules = [...new Set(allIds.flatMap((id) => rulesByDec.get(id) || []))];
  const deep = [];
  for (const doc of [failuresDoc, docsById.get("messaging-and-consistency"), docsById.get("stock-release-safety-nets")].filter(Boolean)) {
    for (const s of doc.sections) {
      const overlap = s.ids.filter((x) => allIds.includes(x)).length;
      if (overlap) deep.push({ doc, s, overlap });
    }
  }
  deep.sort((a, b) => b.overlap - a.overlap);
  const block = (title, items) => (items.length ? `<div class="card"><h4>${title}</h4><ul>${items.join("")}</ul></div>` : "");
  return `<aside class="side">
    ${block("Decisions", allIds.map((id) => `<li>${decLink(id)} <span class="small muted">${esc(truncate(plainText(decById.get(id).decision), 70))}</span></li>`))}
    ${block("Business rules", rules.slice(0, 12).map((r) => `<li><a href="#/rule/${encodeURIComponent(r.num)}">${esc(r.num)}</a> <span class="small muted">${esc(truncate(plainText(r.rule), 70))}</span></li>`))}
    ${block("Failure cases", [...failSet].sort((a, b) => a.slice(1) - b.slice(1)).map((id) => `<li>${failLink(id)} <span class="small muted">${esc(truncate(plainText(failById.get(id).scenario), 70))}</span></li>`))}
    ${block("Technical deep dive", deep.slice(0, 8).map((x) => `<li><a href="${sectionHref(x.doc, x.s.n)}">${esc(x.s.title)}</a> <span class="small muted">${esc(x.doc.title)}</span></li>`))}
  </aside>`;
}

function viewFlow(i) {
  const f = D.flows[+i];
  if (!f) return notFound(`Flow ${i}`);
  const text = sectionMd(flowsDoc, f.section);
  return `
    <div class="row"><div class="crumbs"><a href="#/flows">Business flows</a></div><span class="spacer"></span>${pager("flow", String(i))}</div>
    <div class="side-layout"><div class="doc prose" data-md data-ctx="business-flows">${md(text)}</div>${relatedPanel(text)}</div>`;
}

// Reviews ---------------------------------------------------------------------------------

function viewReviews() {
  state.seq = null;
  const dates = [...new Set(D.reviews.map((r) => r.date))];
  return `<h1>Mentor reviews</h1>
    <p class="lede">Each point a mentor raised, the options weighed, the choice made and why.</p>
    ${dates
      .map((d) => `<div class="card"><h3>${d}</h3><ul>${D.reviews
        .map((r, i) => (r.date === d ? `<li><a href="#/review/${i}">${esc(r.title)}</a></li>` : ""))
        .join("")}</ul></div>`)
      .join("")}
    <p><a href="#/doc/mentor-reviews/">Read the whole record</a></p>`;
}

function viewReview(i) {
  const r = D.reviews[+i];
  if (!r) return notFound(`Review point ${i}`);
  const body = sectionMd(reviewsDoc, r.section, r.title !== "Overview");
  return `
    <div class="row"><div class="crumbs"><a href="#/reviews">Mentor reviews</a> › ${r.date}</div><span class="spacer"></span>${pager("review", String(i))}</div>
    <div class="side-layout"><div class="doc prose" data-md data-ctx="mentor-reviews">${md(body)}</div>${relatedPanel(body)}</div>`;
}

// Documents ---------------------------------------------------------------------------------

function viewDocs() {
  const groups = [...new Set(D.docs.map((d) => d.group))];
  return `<h1>Documents</h1>${groups
    .map((g) => `<h2>${esc(g)}</h2><div class="grid">${D.docs
      .filter((d) => d.group === g)
      .map((d) => `<a class="card" href="#/doc/${d.id}/"><b>${esc(d.title)}</b><div class="small muted">${esc(d.path)}</div></a>`)
      .join("")}</div>`)
    .join("")}`;
}

function viewDoc(id) {
  const doc = docsById.get(id);
  if (!doc) return notFound(`Document ${id}`);
  const toc = doc.sections
    .filter((s) => s.level >= 2 && s.level <= 4)
    .map((s) => `<a class="l${s.level}" href="${sectionHref(doc, s.n)}">${esc(s.title)}</a>`)
    .join("");
  return `<div class="crumbs"><a href="#/docs">Documents</a> › ${esc(doc.group)} · <a href="${vscode(doc.path)}">${esc(doc.path)}</a></div>
    <div class="doc-layout"><div class="doc prose" data-md data-ctx="${doc.id}">${md(doc.md)}</div><nav class="toc">${toc}</nav></div>`;
}

function scrollToAnchor(id, anchor) {
  if (!anchor) return window.scrollTo(0, 0);
  const doc = docsById.get(id);
  const heads = $$(".doc h1, .doc h2, .doc h3, .doc h4, .doc h5, .doc h6");
  let target = null;
  if (anchor.startsWith("@")) {
    const s = doc.sections[+anchor.slice(1)];
    const norm = (t) => t.toLowerCase().replace(/[^\p{L}\p{N}]/gu, "");
    target = heads[s?.n];
    if (!target || norm(target.textContent) !== norm(s.title)) target = heads.find((h) => norm(h.textContent) === norm(s?.title || ""));
  } else target = document.getElementById(decodeURIComponent(anchor));
  if (target) {
    target.scrollIntoView({ block: "start" });
    window.scrollBy(0, -70);
    target.classList.add("flash");
  }
}

// Search ("Ask") ------------------------------------------------------------------------------

const STOP = new Set("a an and are as at be by can do does for from how i if in is it of on or our the this to was we what when where which who why will with without you your there their they its not no any happens happen".split(" "));
const SYNONYMS = {
  double: ["duplicate", "idempot", "twice", "double"], duplicate: ["duplicate", "dedupe", "idempot"], click: ["idempot", "click"],
  down: ["down", "outage", "unavailable"], crash: ["crash", "killed", "restart"], pay: ["payment", "charge"], card: ["card", "payment", "charge", "declin"],
  money: ["payment", "refund", "charge"], slow: ["latency", "timeout", "slow", "bottleneck"], oversell: ["oversell", "last unit", "insufficient"],
  overselling: ["oversell", "last unit", "insufficient"], login: ["login", "token", "jwt"], auth: ["auth", "jwt", "token"], ai: ["assistant", "llm", "ai"],
  search: ["search", "retriev", "hybrid"], scale: ["scal", "bottleneck", "concurren"], limit: ["limit", "rate"],
};
const stem = (w) => (w.length > 5 ? w.replace(/(ing|ed|es|s)$/, "") : w.length > 4 ? w.replace(/s$/, "") : w);
const strip = (s) => String(s || "").replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1").replace(/[`*#|>]/g, " ").replace(/\s+/g, " ");

let corpus = null;
function buildCorpus() {
  const items = [];
  const add = (type, title, body, href, weight, extra = {}) => items.push({ type, title, body: strip(body), lt: strip(title).toLowerCase(), lb: strip(body).toLowerCase(), href, weight, ...extra });
  D.decisions.forEach((d) => add("Decision", `${d.id} ${plainText(d.decision)}`, `${d.why} ${d.alternatives} ${d.revisit} ${d.legacy.join(" ")}`, `#/decision/${d.id}`, 1.3, { dec: d }));
  D.rules.forEach((r) => add("Business rule", `${r.num} ${plainText(r.rule)}`, `${r.why} ${r.source} ${r.alternatives} ${r.ids_raw}`, `#/rule/${encodeURIComponent(r.num)}`, 1.25, { rule: r }));
  D.failures.forEach((f) => add("Failure case", `${f.id} ${plainText(f.scenario)}`, `${f.outcome} ${f.mechanism}`, `#/failure/${f.id}`, 1.3, { fail: f }));
  D.reviews.forEach((r, i) => add("Mentor review", `${r.date} ${r.title}`, sectionMd(reviewsDoc, r.section, false), `#/review/${i}`, 1.1));
  D.flows.forEach((f, i) => add("Business flow", f.title, sectionMd(flowsDoc, f.section), `#/flow/${i}`, 1.2));
  for (const doc of D.docs) {
    if (doc.group === "Archived RFCs") continue;
    for (const s of doc.sections) {
      const body = lines(doc).slice(s.start, s.end).join("\n");
      if (body.trim().length < 40) continue;
      add("Document", `${s.title}`, body, sectionHref(doc, s.n), 0.85, { docTitle: doc.title });
    }
  }
  return items;
}

// BM25-style ranking over substrings: rare terms weigh more, long sections do not win on volume,
// and a synonym counts for less than the word the reviewer typed.
const dfCache = new Map();
function docFreq(term) {
  if (!dfCache.has(term)) dfCache.set(term, corpus.filter((x) => x.lt.includes(term) || x.lb.includes(term)).length);
  return dfCache.get(term);
}
function search(query) {
  corpus ||= buildCorpus();
  const avgLen = (search.avg ||= corpus.reduce((n, x) => n + x.lb.length, 0) / corpus.length);
  const words = query.toLowerCase().match(/[\p{L}\p{N}_-]+/gu) || [];
  const groups = words
    .filter((w) => !STOP.has(w) && w.length > 1)
    .map((w) => [{ t: stem(w), w: 1 }, ...(SYNONYMS[w] || []).filter((t) => t !== stem(w)).map((t) => ({ t, w: 0.6 }))]);
  if (!groups.length) return { groups: [], results: [] };
  const lensBoost = state.lens === "business" ? { "Business rule": 1.3, "Business flow": 1.3 } : { Decision: 1.15, Document: 1.05 };
  const n = corpus.length;
  const results = [];
  for (const item of corpus) {
    let score = 0, matched = 0;
    const norm = 0.25 + 0.75 * (item.lb.length / avgLen);
    for (const alts of groups) {
      let best = 0;
      for (const { t, w } of alts) {
        let tf = 0, at = item.lb.indexOf(t);
        while (at !== -1 && tf < 20) { tf++; at = item.lb.indexOf(t, at + t.length); }
        const inTitle = item.lt.includes(t);
        if (!tf && !inTitle) continue;
        const idf = Math.log(1 + (n - docFreq(t) + 0.5) / (docFreq(t) + 0.5));
        best = Math.max(best, w * idf * ((inTitle ? 2.5 : 0) + (tf * 2.2) / (tf + 1.2 * norm)));
      }
      if (best) { matched++; score += best; }
    }
    const coverage = matched / groups.length;
    if (!matched || (groups.length > 1 && coverage < 0.5)) continue;
    results.push({ item, score: score * coverage * coverage * item.weight * (lensBoost[item.type] || 1) });
  }
  results.sort((a, b) => b.score - a.score);
  return { groups: groups.map((g) => g.map((x) => x.t)), results };
}

function highlight(text, groups, around = 160) {
  const lower = text.toLowerCase();
  const terms = groups.flat().filter((t) => t.length > 2);
  let at = -1;
  for (const t of terms) { const i = lower.indexOf(t); if (i !== -1 && (at === -1 || i < at)) at = i; }
  let out = at === -1 ? text.slice(0, around * 2) : text.slice(Math.max(0, at - around / 2), at + around * 1.5);
  if (at > around / 2) out = "…" + out;
  let html = esc(out.trim());
  for (const t of terms.sort((a, b) => b.length - a.length)) html = html.replace(new RegExp(`(${t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}[\\p{L}\\p{N}]*)`, "giu"), "<mark>$1</mark>");
  return html;
}

function viewAsk(q) {
  const query = q.get("q") || "";
  const type = q.get("type") || "";
  const direct = query.trim().toUpperCase();
  if (decById.has(direct)) return viewDecision(direct);
  if (failById.has(direct)) return viewFailure(direct);
  const { groups, results } = search(query);
  const types = ["Decision", "Business rule", "Business flow", "Failure case", "Mentor review", "Document"];
  const counts = Object.fromEntries(types.map((t) => [t, results.filter((r) => r.item.type === t).length]));
  const shown = (type ? results.filter((r) => r.item.type === type) : results).slice(0, 60);
  const best = results.slice(0, 3);
  const qs = (t) => `#/ask?q=${encodeURIComponent(query)}${t ? `&type=${encodeURIComponent(t)}` : ""}`;
  return `
    <h1>${esc(query) || "Ask"}</h1>
    <p class="lede">${results.length} matches across decisions, rules, flows, failure cases, reviews and every document section.</p>
    ${!type && best.length ? `<h2>Best answers</h2>${best.map((r) => answerCard(r.item, groups)).join("")}` : ""}
    <div class="filters results-type">
      <a class="chip ${type ? "" : "on"}" href="${qs("")}">All ${results.length}</a>
      ${types.filter((t) => counts[t]).map((t) => `<a class="chip ${type === t ? "on" : ""}" href="${qs(t)}">${t} ${counts[t]}</a>`).join("")}
    </div>
    <ul class="list">${shown
      .map((r) => `<li><div class="row"><span class="tag ${["Business rule", "Business flow"].includes(r.item.type) ? "biz" : "tech"}">${r.item.type}</span>
        <a class="title" href="${r.item.href}">${esc(truncate(r.item.title, 140))}</a>${r.item.docTitle ? `<span class="small muted">${esc(r.item.docTitle)}</span>` : ""}</div>
        <div class="small muted">${highlight(r.item.body, groups)}</div></li>`)
      .join("") || '<li class="muted">Nothing matched. Try fewer or different words.</li>'}</ul>`;
}

function answerCard(item, groups) {
  if (item.dec) return decisionCard(item.dec).replace('class="card tech-card"', 'class="card tech-card answer"');
  if (item.rule) return ruleCard(item.rule, true);
  if (item.fail) return failureCard(item.fail);
  return `<div class="card"><div class="row"><span class="tag">${item.type}</span><a href="${item.href}"><b>${esc(item.title)}</b></a>
    ${item.docTitle ? `<span class="small muted">${esc(item.docTitle)}</span>` : ""}</div><p class="small">${highlight(item.body, groups, 320)}</p></div>`;
}

const notFound = (what) => `<h1>Not found</h1><p>${esc(what)} is not in the data. Rebuild with <code>make review-ui</code> if the docs changed.</p>`;

// --- Navigation and routing ------------------------------------------------------------

function renderNav(active) {
  const a = (href, label, count, on) => `<a href="${href}" class="${on ? "on" : ""}"><span>${label}</span>${count != null ? `<span class="count">${count}</span>` : ""}</a>`;
  const [view, arg] = active;
  const svc = new URLSearchParams(location.hash.split("?")[1] || "").get("svc");
  const decPrefix = view === "decision" ? arg?.split("-")[0] : view === "decisions" ? svc : null;
  $("#nav").innerHTML = `
    ${a("#/", "Overview", null, !view)}
    <div class="heading">Business</div>
    ${a("#/flows", "Business flows", D.flows.length, view === "flows" || view === "flow")}
    ${a("#/rules", "Business rules", D.rules.length, view === "rules" || view === "rule")}
    ${a("#/rules?only=silent", "Requirement silent", D.rules.filter((r) => r.silent).length, false)}
    <div class="heading">Technical</div>
    ${a("#/decisions", "Decisions", D.decisions.length, (view === "decisions" && !svc) || false)}
    <div class="sub">${D.services.map((s) => a(`#/decisions?svc=${s.prefix}`, esc(s.name), s.count, decPrefix === s.prefix)).join("")}</div>
    ${a("#/failures", "Failure cases", D.failures.length, view === "failures" || view === "failure")}
    ${a("#/doc/architecture-diagrams/", "Architecture diagrams", null, view === "doc" && arg === "architecture-diagrams")}
    <div class="heading">Review</div>
    ${a("#/reviews", "Mentor reviews", D.reviews.length, view === "reviews" || view === "review")}
    ${a("#/docs", "All documents", D.docs.length, view === "docs" || (view === "doc" && arg !== "architecture-diagrams"))}
    <p class="small muted" style="padding:14px 10px">Built ${esc(D.built)}<br>${esc(D.git.branch)} @ ${esc(D.git.commit)}</p>`;
}

const FILLERS = { decisions: fillDecisions, rules: fillRules, failures: fillFailures };

function route() {
  const raw = location.hash.replace(/^#\/?/, "");
  const [path, qs] = raw.split("?");
  const parts = path.split("/").map(decodeURIComponent);
  const q = new URLSearchParams(qs || "");
  const [view, arg, anchor] = parts;
  pager.current = null;
  const views = {
    "": () => viewHome(), ask: () => viewAsk(q), decisions: () => viewDecisions(q), decision: () => viewDecision(arg),
    rules: () => viewRules(q), rule: () => viewRule(arg), failures: () => viewFailures(q), failure: () => viewFailure(arg),
    flows: () => viewFlows(), flow: () => viewFlow(arg), reviews: () => viewReviews(), review: () => viewReview(arg),
    docs: () => viewDocs(), doc: () => viewDoc(arg),
  };
  const main = $("#main");
  main.innerHTML = (views[view] || (() => notFound(raw)))();
  if (FILLERS[view]) $("[data-results]", main).innerHTML = FILLERS[view](q);
  const rendering = $$("[data-md]", main).map((el) => enhance(el, docsById.get(el.dataset.ctx)));
  $$(".list", main).forEach(linkify);
  renderNav([view, arg]);
  $("#ask-input").value = view === "ask" ? q.get("q") || "" : "";
  if (view === "doc") {
    scrollToAnchor(arg, anchor);
    // Diagrams above the target change the page height once they render.
    if (anchor) Promise.all(rendering).then(() => scrollToAnchor(arg, anchor));
  } else window.scrollTo(0, 0);
}

function updateQuery(key, value) {
  const [path, qs] = location.hash.split("?");
  const q = new URLSearchParams(qs || "");
  if (value) q.set(key, value);
  else q.delete(key);
  return `${path}${q.toString() ? "?" + q : ""}`;
}

document.addEventListener("click", (e) => {
  const chip = e.target.closest("[data-filter]");
  if (chip) location.hash = updateQuery(chip.dataset.filter, chip.dataset.value);
  const ask = e.target.closest("[data-ask]");
  if (ask) location.hash = `#/ask?q=${encodeURIComponent(ask.dataset.ask)}`;
});

let filterTimer;
document.addEventListener("input", (e) => {
  if (!e.target.matches("[data-filter-text]")) return;
  clearTimeout(filterTimer);
  filterTimer = setTimeout(() => {
    history.replaceState(null, "", updateQuery("q", e.target.value.trim()));
    const view = location.hash.replace(/^#\/?/, "").split(/[/?]/)[0];
    const results = $("[data-results]");
    results.innerHTML = FILLERS[view](new URLSearchParams(location.hash.split("?")[1] || ""));
    linkify(results);
  }, 120);
});

document.addEventListener("submit", (e) => {
  e.preventDefault();
  const input = $("input", e.target);
  if (input.value.trim()) location.hash = `#/ask?q=${encodeURIComponent(input.value.trim())}`;
});

document.addEventListener("keydown", (e) => {
  if (e.target.matches("input, textarea")) {
    if (e.key === "Escape") e.target.blur();
    return;
  }
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  if (e.key === "/") { e.preventDefault(); $("#ask-input").focus(); $("#ask-input").select(); }
  else if (e.key === "ArrowLeft" && pager.current?.prev) location.hash = pager.current.prev;
  else if (e.key === "ArrowRight" && pager.current?.next) location.hash = pager.current.next;
  else if (e.key === "t") toggleTheme();
  else if (e.key === "b") setLens(state.lens === "business" ? "technical" : "business");
});

$$(".lens button").forEach((b) => b.addEventListener("click", () => setLens(b.dataset.lens)));
function toggleTheme() {
  setTheme(state.theme === "dark" ? "light" : "dark");
  route(); // diagrams pick up the new theme only when drawn again
}
$("#theme").addEventListener("click", toggleTheme);
window.addEventListener("hashchange", route);

setTheme(state.theme);
$$(".lens button").forEach((b) => b.classList.toggle("on", b.dataset.lens === state.lens));
route();
