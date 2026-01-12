# 🚀 POP VCS

<p align="center">
  <b>A lightweight, layered version control system</b><br/>
  Simple • Predictable • Hackable
</p>

<p align="center">
  <a href="https://github.com/Bluezly/POP-VCS/blob/main/docs/index.md">📘 Documentation</a> |
  <a href="#installation">⚙️ Installation</a> |
  <a href="#philosophy">🧠 Philosophy</a>
</p>

---

## ✨ What is POP?

**POP** is a lightweight, layered version control system with an optional self-hosted server.

It is designed for developers who:
- Want a VCS they can **understand and modify**
- Prefer **clarity over magic**
- Need fast, predictable daily workflows

POP is **not a Git replacement**.  
It is an alternative for cases where Git feels heavier than necessary.

---

## 🎯 Motivation

Modern VCS tools are extremely powerful, but that power comes with complexity:

- Large and opaque internal formats  
- Complex network negotiation  
- Steep learning curve for contributors  

POP takes a different path:

> Build a VCS with a **small, readable core** that solves common problems well.

---

## 🧩 Core Ideas

### 🧱 Layered Commits
- Changes are stored as layers on top of a base snapshot
- Automatic squashing keeps history compact and readable

### ⚡ Fast Local Operations
- Parallel directory scanning
- Metadata cache (mtime + size)
- Skips rehashing unchanged files

### 📦 Simple Object Store
- Content-addressed storage
- Predictable formats
- Zstandard (zstd) compression

### 🌐 Straightforward Remotes
- HTTP and SSH support
- Minimal protocol complexity
- Easy self-hosting

### 🔧 One Binary
- CLI tool
- Optional server mode
- No external services required

---

## ✅ What POP Is Good At

POP works best for:
- Personal projects
- Small to medium teams
- Private repositories
- Developers who value simplicity
- Environments where self-hosting matters

---

## ❌ What POP Is Not

POP intentionally does **not** aim to be:
- A full Git ecosystem replacement
- An enterprise-scale VCS
- A tool for deep historical analysis
- A drop-in replacement for GitHub workflows

If you rely heavily on features like `blame`, `bisect`, or massive public ecosystems,  
**Git remains the right tool.**

---

## ⚙️ Installation

### Build from source

```bash
cargo build --release
```

### Build with server support

```bash
cargo build --release --features server
```

---

## 🛠️ Basic Workflow

```bash
pop init
pop add .
pop commit -m "Initial commit"
pop status
pop log
```

---

## 🌿 Branching & Navigation

```bash
pop branch feature-x
pop checkout main
pop diff
```

---

## 🔄 Remote Usage

```bash
pop push <url> --repo myrepo
pop pull <url> --repo myrepo
pop checkout main --remote <url> --repo myrepo
```

Supported remotes:
- HTTP
- SSH

---

## 🖥️ Server Mode

```bash
pop serve --dir /data/pop --addr 0.0.0.0:8787
```

Server features:
- Repository hosting
- Object storage
- Push / pull
- Optional token authentication

---

## 📘 Documentation

Full documentation is available here:

👉 https://github.com/Bluezly/POP-VCS/blob/main/docs/index.md

---

## 📌 Project Status

POP is functional and actively evolving.

Planned improvements:
- Streamed object transfer
- Batch synchronization
- Faster storage layouts
- Clearer permission and lock rules

---

## 📄 License

MIT

---

## 🧠 Philosophy

POP exists because not every project needs the full complexity of Git.

If you want a version control system you can:
- read
- reason about
- and extend

**POP is for you.**
