# R0ice Community Reader

A small background app that lets a volunteer's idle GPU help read and summarize public-domain wisdom books (Aristotle, Marcus Aurelius, Franklin, and more) for [R0ice's Wisdom Farm](https://r0ice.com/resource-team/) -- the same spirit as Folding@home or BOINC, just for books instead of proteins.

This is the real, running source -- what you read here is exactly what a volunteer's machine executes. Nothing is hidden.

## What it does

1. Reports basic hardware info (CPU, RAM, GPU model + VRAM) so the server knows what a machine can handle.
2. If [Ollama](https://ollama.com) is installed (or gets auto-installed with consent-free, silent setup), it asks the server for a small chunk of book text, summarizes it locally on the volunteer's own GPU, and sends the summary back. That's it -- no other access to the machine, no other data collected.
3. Runs quietly with a tray icon and a local status page (`http://127.0.0.1:8477`) so you always know exactly what it's doing and can pause any time.
4. Auto-pauses while OBS is streaming/recording, and during a configurable daily quiet window -- the GPU always belongs to what the owner is actually doing first.

## What it never does

- Never reads your files, browser history, or anything outside the one background task described above.
- Never runs arbitrary code from the server -- it only ever receives raw book text and returns a plain-text summary.
- Never phones home with anything beyond hardware specs and reading throughput.

## Building

```
npm install
node index.js              # run from source
npx pkg . --targets node22-win-x64   # build a standalone .exe (Windows)
```

## License

MIT -- see [LICENSE](./LICENSE).

## Why this exists

Part of the [R0ice Wisdom Farm](https://r0ice.com/resource-team/) project, which is slowly building a cross-referenced, citation-verified archive of public-domain wisdom literature. Distributed volunteer compute handles the summarization workload; the interesting part is the archive it produces, not the compute itself.
