# Review explorer

A local web page for mentor and design reviews. It answers "what did we decide,
why, what did we reject, and where is it in the code?" from the documents that
already exist, so it never drifts from them.

```bash
make review-ui          # build, then serve on http://localhost:8077
```

Or build once and open `site/index.html` directly in a browser:

```bash
python3 tools/review_explorer/build.py
```

## What it reads

| Source | Becomes |
| --- | --- |
| Decision register (§3.2.1) of every RFC in `docs/rfcs/` | Decisions: what, why, alternatives rejected, revisit if |
| `docs/business-logic-and-decisions.md` rule tables | Business rules, with the requirement each comes from, linked to decisions |
| `docs/business-flows.md` | Business flows, with their diagrams |
| `docs/distributed-systems-and-failure-modes.md` §5 | Failure cases, with the tests that prove them |
| `docs/reviews/mentor-reviews.md` | Mentor review points |
| Code, config and tests that name a decision ID | The code path of that decision |
| Every page under `docs/` | Full-text search and the document reader |

Nothing is copied by hand. Change a doc or the code, run `make review-ui` again.

## Using it in a review

- **Ask box** (`/`): plain questions such as "what if Kafka is down" or "why no
  cart". Typing an ID (`ORD-D36`, `F12`) jumps straight to it.
- **Business / Technical lens** (`b`): changes the overview and which tab a
  detail page opens on. Business shows rules, requirement sources and flows;
  Technical shows code, tests and failure cases.
- **Detail pages** page through the list you came from with `←` and `→`.
- Every decision or failure ID in any text is a link; hover for a summary.
- **Open in VS Code** links jump to the exact line.
- `t` toggles a dark theme for projectors.

## Notes

- `marked` and `mermaid` load from a CDN. Offline, text still shows, but
  unformatted and without diagrams.
- `site/data.js` is generated and gitignored. It holds the absolute repo path
  for the VS Code links.
- A decision shows code only when the code names its ID in a comment or
  docstring. Adding the ID to the code that implements a decision is
  what makes it appear here.
