use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use clap::{Args, Parser, Subcommand};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(feature = "server")]
use axum::{
    extract::{Path as AxPath, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};

#[cfg(feature = "server")]
use tokio::net::TcpListener;

#[cfg(feature = "server")]
use tracing::{info, warn};

#[cfg(feature = "server")]
use tracing_subscriber::EnvFilter;
use rand::RngCore;

const POP_DIR: &str = ".pop";
const ZSTD_LEVEL: i32 = 6;
const CHUNK_SIZE: usize = 4 * 1024 * 1024;
const LAYER_SQUASH_THRESHOLD: usize = 32;
const MAX_JSON_SIZE: u64 = 100 * 1024 * 1024;

#[derive(Parser)]
#[command(name="pop", version="0.10.2")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Init,
    Add { paths: Vec<String> },
    Rm { paths: Vec<String> },
    Status,
    Commit { #[arg(short='m')] message: String },
    Log { #[arg(long)] n: Option<usize> },
    Checkout { target: String, #[arg(long)] remote: Option<String>, #[arg(long)] repo: Option<String>, #[arg(long)] token: Option<String>, #[arg(long)] ssh_root: Option<String> },
    Branch { name: String },
    Diff { #[arg(long)] from: Option<String>, #[arg(long)] to: Option<String> },
    Merge { branch: String },
    CherryPick { commit: String },
    Rebase { onto: String },
    Reflog,
    Fsck,
    Gc,
    Sparse { #[command(subcommand)] cmd: SparseCmd },
    Ignore { #[command(subcommand)] cmd: IgnoreCmd },

    #[cfg(feature = "server")]
    Serve(ServeArgs),

  Lock { path: String, #[arg(long)] remote: Option<String>, #[arg(long)] repo: Option<String>, #[arg(long)] token: Option<String>, #[arg(long)] ssh_root: Option<String>, #[arg(long, default_value_t=3600)] ttl: u64 },
  Unlock { path: String, #[arg(long)] remote: Option<String>, #[arg(long)] repo: Option<String>, #[arg(long)] token: Option<String>, #[arg(long)] ssh_root: Option<String> },
  Locks { #[arg(long)] remote: Option<String>, #[arg(long)] repo: Option<String>, #[arg(long)] token: Option<String>, #[arg(long)] ssh_root: Option<String> },
  Renew { path: String, #[arg(long)] remote: Option<String>, #[arg(long)] repo: Option<String>, #[arg(long)] token: Option<String>, #[arg(long)] ssh_root: Option<String>, #[arg(long, default_value_t=3600)] ttl: u64 },


    Push(RemoteArgs),
    Pull(RemoteArgs),
}

#[derive(Subcommand)]
enum SparseCmd {
    Set { paths: Vec<String> },
    Clear,
    Show,
}

#[derive(Subcommand)]
enum IgnoreCmd {
    Add { pattern: String },
    Show,
}

#[cfg(feature = "server")]
#[derive(Args)]
struct ServeArgs {
    #[arg(long, default_value = "0.0.0.0:8787")]
    addr: String,
    #[arg(long)]
    dir: String,
    #[arg(long)]
    token: Option<String>,
}

#[derive(Args, Clone)]
struct RemoteArgs {
    url: String,
    #[arg(long)]
    repo: String,
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    ssh_root: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct Config {
    ignore: Vec<String>,
    sparse: Vec<String>,
    lock_required: Vec<String>,
}


#[derive(Serialize, Deserialize, Clone, Default)]
struct LockDb {
    locks: BTreeMap<String, LockEntry>, 
}

#[derive(Serialize, Deserialize, Clone)]
struct LockEntry {
    path: String,
    owner: String,
    token: String,
    ts: i64,
    ttl: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct LockReq {
    path: String,
    owner: String,
    token: String,
    ttl: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct LockResp {
    ok: bool,
    message: String,
    locks: Vec<LockEntry>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct Head {
    r#ref: Option<String>,
    detached: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct Index {
    put: BTreeMap<String, IndexEntry>,
    del: BTreeSet<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct IndexEntry {
    hash: String,
    size: u64,
    mtime: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct Commit {
    id: String,
    parents: Vec<String>,
    message: String,
    seq: u64,
    base_root: String,
    layers: Vec<String>,
    effective_root: String,
    ts: i64,
}

#[derive(Serialize, Deserialize, Clone)]
struct TreeEntry {
    name: String,
    kind: String,
    hash: String,
    mode: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Tree {
    entries: Vec<TreeEntry>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag="op", rename_all="lowercase")]
enum LayerChange {
    Put { path: String, hash: String },
    Del { path: String },
}

#[derive(Serialize, Deserialize, Clone)]
struct Layer {
    changes: Vec<LayerChange>,
}

#[derive(Serialize, Deserialize, Clone)]
struct ReflogEntry {
    ts: i64,
    old: String,
    new: String,
    action: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct RemoteBundle {
    commits: Vec<Commit>,
    refs: BTreeMap<String, String>,
    head: Head,
    config: Config,
    reflog: Vec<ReflogEntry>,
    seq: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct OkResp { ok: bool }

#[derive(Serialize, Deserialize, Clone)]
struct PullResp { ok: bool, bundle: RemoteBundle }

#[derive(Serialize, Deserialize, Clone)]
struct InfoResp { ok: bool, repos: Vec<String> }

#[derive(Serialize, Deserialize, Clone)]
struct ObjResp { ok: bool, bytes_b64: String }

#[derive(Serialize, Deserialize, Clone)]
struct PutBytesReq { bytes_b64: String }

#[derive(Serialize, Deserialize, Clone)]
struct PushReq { repo: String, bundle: RemoteBundle }

#[derive(Clone, Default)]
struct Cache {
    flat_tree: Arc<Mutex<HashMap<String, Arc<BTreeMap<String, String>>>>>,
    eff_commit: Arc<Mutex<HashMap<String, Arc<BTreeMap<String, String>>>>>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct StatCache {
    files: BTreeMap<String, IndexEntry>,
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn file_mtime_size(p: &Path) -> Result<(u64, u64)> {
    let md = fs::metadata(p)?;
    let size = md.len();
    let mtime = md.modified().ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok((mtime, size))
}

fn repo_paths() -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf, PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    let root = PathBuf::from(POP_DIR);
    let objects = root.join("objects");
    let commits = root.join("commits");
    let refs = root.join("refs").join("heads");
    let head_json = root.join("HEAD.json");
    let index_json = root.join("index.json");
    let cfg_json = root.join("config.json");
    let seq = root.join("SEQ");
    let reflog = root.join("reflog.json");
    let statcache = root.join("statcache.json");
    (root, objects, commits, refs, head_json, index_json, cfg_json, seq, reflog, statcache)
}

fn ensure_repo() -> Result<()> {
    let pop_dir = Path::new(POP_DIR);
    if !pop_dir.exists() {
        bail!(
            "Not a pop repository!\nCould not find '{}' directory in current location: {}\nRun 'pop init' to initialize a new repository.",
            POP_DIR,
            std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_else(|_| "<unknown>".into()),
        );
    }
    Ok(())
}

fn locks_path() -> PathBuf {
    PathBuf::from(POP_DIR).join("locks.json")
}

fn read_locks() -> LockDb {
    read_json(&locks_path()).unwrap_or_default()
}

fn write_locks(db: &LockDb) -> Result<()> {
    write_json(&locks_path(), db)
}

fn purge_expired(db: &mut LockDb) {
    let now = now_unix();
    db.locks.retain(|_, e| now <= e.ts + (e.ttl as i64));
}

fn owner_string() -> String {
    if let Ok(u) = std::env::var("POP_USER") { if !u.trim().is_empty() { return u; } }
    let user = std::env::var("USER").or_else(|_| std::env::var("USERNAME")).unwrap_or_else(|_| "unknown".into());
    let host = std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")).unwrap_or_else(|_| "host".into());
    format!("{}@{}", user, host)
}

fn lock_token() -> String {
    let mut b = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut b);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

fn read_json<T: for<'de> Deserialize<'de>>(p: &Path) -> Result<T> {
    let md = fs::metadata(p).with_context(|| format!("stat {:?}", p))?;
    if md.len() > MAX_JSON_SIZE {
        bail!("file too large: {:?} ({} bytes)", p, md.len());
    }
    let b = fs::read(p).with_context(|| format!("read {:?}", p))?;
    Ok(serde_json::from_slice(&b)?)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(par) = path.parent() { fs::create_dir_all(par)?; }
    let tmp = path.with_extension(format!("tmp_{}", now_unix()));
    {
        let mut f = fs::OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all().ok();
    }
    fs::rename(&tmp, path).or_else(|_| {
        fs::remove_file(path).ok();
        fs::rename(&tmp, path)
    }).with_context(|| format!("rename {:?} -> {:?}", tmp, path))?;
    Ok(())
}

fn write_json<T: Serialize>(p: &Path, v: &T) -> Result<()> {
    let b = serde_json::to_vec_pretty(v)?;
    atomic_write(p, &b).with_context(|| format!("write {:?}", p))?;
    Ok(())
}

fn requires_lock(path: &str, cfg: &Config) -> bool {
    if cfg.lock_required.is_empty() { return false; }
    if let Ok(gs) = build_globset(&cfg.lock_required) {
        return gs.is_match(path);
    }
    false
}

fn assert_locks_for_index(idx: &Index, cfg: &Config) -> Result<()> {
    let mut db = read_locks();
    purge_expired(&mut db);
    let me = owner_string();

    let mut needed = vec![];
    for p in idx.put.keys() {
        if requires_lock(p, cfg) {
            needed.push(p.clone());
        }
    }
    for p in &idx.del {
        if requires_lock(p, cfg) {
            needed.push(p.clone());
        }
    }
    needed.sort(); needed.dedup();

    let mut blocked = vec![];
    for p in needed {
        match db.locks.get(&p) {
            None => blocked.push(format!("{} (missing lock)", p)),
            Some(e) if e.owner != me => blocked.push(format!("{} (locked by {})", p, e.owner)),
            Some(_) => {}
        }
    }

    if !blocked.is_empty() {
        bail!(
            "binary locking required:\n{}\n\nUse:\n  pop lock <path>\n(or pop lock <path> --remote ... --repo ...)\n",
            blocked.into_iter().map(|s| format!("  - {}", s)).collect::<Vec<_>>().join("\n")
        );
    }

    write_locks(&db).ok();
    Ok(())
}


fn init_repo() -> Result<()> {
    if Path::new(POP_DIR).exists() { bail!(".pop already exists"); }
    let (_root, objects, commits, refs, head_json, index_json, cfg_json, seq_path, reflog_path, statcache_path) = repo_paths();
    fs::create_dir_all(&objects)?;
    fs::create_dir_all(&commits)?;
    fs::create_dir_all(&refs)?;
    let cfg = Config {
        ignore: vec![POP_DIR.into(), ".git".into(), "node_modules".into(), "target".into(), "dist".into(), "build".into()],
        sparse: vec![],
     lock_required: vec![
    "*.blend".into(), "*.fbx".into(), "*.psd".into(), "*.ai".into(),
    "*.uasset".into(), "*.umap".into(), "*.unity".into(), "*.scene".into(),
    "*.kra".into(), "*.clip".into()
  ],
};
    write_json(&cfg_json, &cfg)?;
    write_json(&head_json, &Head { r#ref: Some("refs/heads/main".into()), detached: None })?;
    write_json(&index_json, &Index::default())?;
    atomic_write(&seq_path, b"0\n")?;
    write_json(&reflog_path, &Vec::<ReflogEntry>::new())?;
    write_json(&statcache_path, &StatCache::default())?;
    let empty_root = store_tree(&Tree { entries: vec![] })?;
    let c0 = Commit {
        id: "cmt_0000000".into(),
        parents: vec![],
        message: "initial".into(),
        seq: 0,
        base_root: empty_root.clone(),
        layers: vec![],
        effective_root: empty_root,
        ts: now_unix(),
    };
    write_commit(&c0)?;
    atomic_write(&refs.join("main"), format!("{}\n", c0.id).as_bytes())?;
    append_reflog("", &c0.id, "init")?;
    Ok(())
}

fn compress(bytes: &[u8]) -> Result<Vec<u8>> { Ok(zstd::encode_all(bytes, ZSTD_LEVEL)?) }
const MAX_DECOMPRESSED_SIZE: u64 = 1024 * 1024 * 1024;

fn decompress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = zstd::Decoder::new(bytes)?;
    let mut result = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut total_read = 0u64;
    
    loop {
        match decoder.read(&mut buffer)? {
            0 => break,
            n => {
                total_read += n as u64;
                if total_read > MAX_DECOMPRESSED_SIZE {
                    bail!("decompressed size exceeds limit of {} bytes", MAX_DECOMPRESSED_SIZE);
                }
                result.extend_from_slice(&buffer[..n]);
            }
        }
    }
    
    Ok(result)
}
fn blake3_hex(bytes: &[u8]) -> String { hex::encode(blake3::hash(bytes).as_bytes()) }

fn obj_path(hash: &str) -> PathBuf {
    let (_root, objects, _commits, _refs, _head_json, _index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    if hash.len() < 2 { return objects.join("xx").join(hash); }
    objects.join(&hash[0..2]).join(hash)
}

fn store_raw_object(raw: &[u8]) -> Result<String> {
    let hash = blake3_hex(raw);
    let p = obj_path(&hash);
    if p.exists() { return Ok(hash); }
    if let Some(par) = p.parent() { fs::create_dir_all(par)?; }
    let c = compress(raw)?;
    atomic_write(&p, &c)?;
    Ok(hash)
}

fn load_raw_object(hash: &str) -> Result<Vec<u8>> {
    let p = obj_path(hash);
    let c = fs::read(p)?;
    decompress(&c)
}

fn store_chunk(bytes: &[u8]) -> Result<String> {
    let mut raw = Vec::with_capacity(bytes.len() + 16);
    raw.extend_from_slice(b"chunk\n");
    raw.extend_from_slice(bytes);
    store_raw_object(&raw)
}

fn load_chunk(hash: &str) -> Result<Vec<u8>> {
    let raw = load_raw_object(hash)?;
    if !raw.starts_with(b"chunk\n") { bail!("not a chunk"); }
    Ok(raw[6..].to_vec())
}

#[derive(Serialize, Deserialize, Clone)]
struct BigBlob {
    size: u64,
    chunks: Vec<String>,
}

fn store_blob(bytes: &[u8]) -> Result<String> {
    if bytes.len() <= CHUNK_SIZE {
        let mut raw = Vec::with_capacity(bytes.len() + 16);
        raw.extend_from_slice(b"blob\n");
        raw.extend_from_slice(bytes);
        return store_raw_object(&raw);
    }
    let mut chunks = vec![];
    for part in bytes.chunks(CHUNK_SIZE) {
        let h = store_chunk(part)?;
        chunks.push(h);
    }
    let meta = BigBlob { size: bytes.len() as u64, chunks };
    let jb = serde_json::to_vec(&meta)?;
    let mut raw = Vec::with_capacity(jb.len() + 16);
    raw.extend_from_slice(b"big\n");
    raw.extend_from_slice(&jb);
    store_raw_object(&raw)
}

fn load_blob(hash: &str) -> Result<Vec<u8>> {
    let raw = load_raw_object(hash)?;
    if raw.starts_with(b"blob\n") { return Ok(raw[5..].to_vec()); }
    if raw.starts_with(b"big\n") {
        let meta: BigBlob = serde_json::from_slice(&raw[4..])?;
        let mut out = Vec::with_capacity(meta.size as usize);
        for ch in meta.chunks {
            let b = load_chunk(&ch)?;
            out.extend_from_slice(&b);
        }
        return Ok(out);
    }
    bail!("not a blob/big")
}

fn store_tree(t: &Tree) -> Result<String> {
    let mut t2 = t.clone();
    t2.entries.sort_by(|a, b| a.name.cmp(&b.name));
    let bytes = serde_json::to_vec(&t2)?;
    let mut raw = Vec::with_capacity(bytes.len() + 16);
    raw.extend_from_slice(b"tree\n");
    raw.extend_from_slice(&bytes);
    store_raw_object(&raw)
}

fn load_tree(hash: &str) -> Result<Tree> {
    let raw = load_raw_object(hash)?;
    if !raw.starts_with(b"tree\n") { bail!("not a tree"); }
    Ok(serde_json::from_slice(&raw[5..])?)
}

fn store_layer(layer: &Layer) -> Result<String> {
    let bytes = serde_json::to_vec(layer)?;
    let mut raw = Vec::with_capacity(bytes.len() + 16);
    raw.extend_from_slice(b"layer\n");
    raw.extend_from_slice(&bytes);
    store_raw_object(&raw)
}

fn load_layer(hash: &str) -> Result<Layer> {
    let raw = load_raw_object(hash)?;
    if !raw.starts_with(b"layer\n") { bail!("not a layer"); }
    Ok(serde_json::from_slice(&raw[6..])?)
}

fn commit_path(id: &str) -> PathBuf {
    let (_root, _objects, commits, _refs, _head_json, _index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    commits.join(format!("{}.json", id))
}
fn write_commit(c: &Commit) -> Result<()> { write_json(&commit_path(&c.id), c) }
fn read_commit(id: &str) -> Result<Commit> { read_json(&commit_path(id)) }

fn read_head() -> Result<Head> {
    let (_root, _objects, _commits, _refs, head_json, _index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    read_json(&head_json)
}
fn write_head(h: &Head) -> Result<()> {
    let (_root, _objects, _commits, _refs, head_json, _index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    write_json(&head_json, h)
}

fn read_ref(ref_path: &str) -> Result<String> {
    let p = PathBuf::from(POP_DIR).join(ref_path.replace('/', &std::path::MAIN_SEPARATOR.to_string()));
    Ok(fs::read_to_string(p)?.lines().next().unwrap_or("").trim().to_string())
}
fn write_ref(ref_path: &str, id: &str) -> Result<()> {
    let p = PathBuf::from(POP_DIR).join(ref_path.replace('/', &std::path::MAIN_SEPARATOR.to_string()));
    if let Some(par) = p.parent() { fs::create_dir_all(par)?; }
    atomic_write(&p, format!("{}\n", id).as_bytes())?;
    Ok(())
}

fn current_commit_id() -> Result<String> {
    let h = read_head()?;
    if let Some(d) = h.detached { return Ok(d); }
    let r = h.r#ref.ok_or_else(|| anyhow!("HEAD invalid"))?;
    read_ref(&r)
}

fn read_seq() -> Result<u64> {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, _cfg_json, seq_path, _reflog_path, _statcache) = repo_paths();
    Ok(fs::read_to_string(seq_path)?.trim().parse()?)
}
fn write_seq(v: u64) -> Result<()> {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, _cfg_json, seq_path, _reflog_path, _statcache) = repo_paths();
    atomic_write(&seq_path, format!("{}\n", v).as_bytes())?;
    Ok(())
}

fn with_seq_lock<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let lock = PathBuf::from(POP_DIR).join("SEQ.lock");
    let pid = std::process::id();
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);
    let mut attempts = 0;
    
    loop {
        match fs::OpenOptions::new().write(true).create_new(true).open(&lock) {
            Ok(mut file) => {
                let lock_data = format!("pid:{}\ntime:{}\n", pid, now_unix());
                let _ = file.write_all(lock_data.as_bytes());
                let out = f();
                let _ = fs::remove_file(&lock);
                return out;
            }
            Err(_) => {
                if start.elapsed() > timeout {
                    if let Ok(content) = fs::read_to_string(&lock) {
                        eprintln!("Lock held by: {}", content);
                    }
                    bail!("could not acquire SEQ lock after {:?} ({} attempts)", timeout, attempts);
                }
                attempts += 1;
                let backoff = std::cmp::min(attempts * 5, 100);
                std::thread::sleep(Duration::from_millis(backoff));
            }
        }
    }
}

fn next_seq() -> Result<u64> {
    with_seq_lock(|| {
        let v = read_seq()?;
        let nv = v + 1;
        write_seq(nv)?;
        Ok(nv)
    })
}

fn read_index() -> Result<Index> {
    let (_root, _objects, _commits, _refs, _head_json, index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    if !index_json.exists() { return Ok(Index::default()); }
    read_json(&index_json)
}
fn write_index(idx: &Index) -> Result<()> {
    let (_root, _objects, _commits, _refs, _head_json, index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    write_json(&index_json, idx)
}

fn read_config() -> Result<Config> {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    if !cfg_json.exists() {
        return Ok(Config::default());
    }
    read_json(&cfg_json)
}

fn read_config_or_default() -> Config {
    read_config().unwrap_or_default()
}
fn write_config(cfg: &Config) -> Result<()> {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    write_json(&cfg_json, cfg)
}

fn read_reflog() -> Result<Vec<ReflogEntry>> {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, _cfg_json, _seq_path, reflog_path, _statcache) = repo_paths();
    if !reflog_path.exists() { return Ok(vec![]); }
    read_json(&reflog_path)
}
fn write_reflog(v: &Vec<ReflogEntry>) -> Result<()> {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, _cfg_json, _seq_path, reflog_path, _statcache) = repo_paths();
    write_json(&reflog_path, v)
}
fn append_reflog(old: &str, new: &str, action: &str) -> Result<()> {
    let mut v = read_reflog()?;
    v.push(ReflogEntry { ts: now_unix(), old: old.to_string(), new: new.to_string(), action: action.to_string() });
    write_reflog(&v)
}

fn read_statcache() -> StatCache {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, _cfg_json, _seq_path, _reflog_path, statcache) = repo_paths();
    read_json(&statcache).unwrap_or_default()
}
fn write_statcache(sc: &StatCache) -> Result<()> {
    let (_root, _objects, _commits, _refs, _head_json, _index_json, _cfg_json, _seq_path, _reflog_path, statcache) = repo_paths();
    write_json(&statcache, sc)
}

fn normalize_path(p: &Path) -> Result<String> {
    let s = p.to_string_lossy().replace('\\', "/");
    let s = s.trim_start_matches("./").to_string();
    
    if s.is_empty() || s == "." {
        return Ok(String::new());
    }
    
    for seg in s.split('/') {
        if seg == ".." || seg.contains('\0') || seg.is_empty() {
            bail!("invalid path component: {:?}", seg);
        }
    }
    
    let canonical = PathBuf::from(&s).canonicalize().ok();
    let current = std::env::current_dir()?;
    
    if let Some(can) = canonical {
        if !can.starts_with(&current) {
            bail!("path outside repository: {:?}", s);
        }
    }
    
    Ok(s)
}

fn is_under_pop(path: &str) -> bool {
    path == POP_DIR || path.starts_with(&format!("{}/", POP_DIR))
}

fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut b = GlobSetBuilder::new();
    for p in patterns { b.add(Glob::new(p)?); }
    Ok(b.build()?)
}

fn should_ignore(path: &str, cfg: &Config) -> bool {
    if path.is_empty() || path == "." { return true; }
    if is_under_pop(path) { return true; }
    for pat in &cfg.ignore {
        let pat = pat.replace('\\', "/").trim().to_string();
        if pat.is_empty() { continue; }
        if path == pat || path.starts_with(&(pat.clone() + "/")) { return true; }
        if pat.starts_with("*.") {
            if let Some(ext) = path.rsplit('.').next() {
                if format!("*.{}", ext) == pat { return true; }
            }
        }
        if pat.ends_with("/*") {
            let base = pat.trim_end_matches("/*");
            if path.starts_with(&(base.to_string() + "/")) { return true; }
        }
    }
    if cfg.sparse.is_empty() { return false; }
    if let Ok(gs) = build_globset(&cfg.sparse) {
        return !gs.is_match(path);
    }
    false
}

fn walk_dir(dir: &Path, cfg: &Config, out: &mut Vec<String>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        let np = match normalize_path(&p) {
            Ok(path) => path,
            Err(_) => continue,
        };
        if np.is_empty() { continue; }
        if should_ignore(&np, cfg) { continue; }
        if entry.file_type()?.is_dir() {
            walk_dir(&p, cfg, out)?;
        } else {
            out.push(np);
        }
    }
    Ok(())
}

fn scan_files(input: &[String]) -> Result<Vec<String>> {
    let cfg = read_config_or_default();
    let mut out = vec![];
    if input.is_empty() {
        walk_dir(Path::new("."), &cfg, &mut out)?;
    } else {
        for s in input {
            let p = PathBuf::from(s);
            let md = fs::metadata(&p).with_context(|| format!("cannot access path: {}", s))?;
            if md.is_dir() {
                walk_dir(&p, &cfg, &mut out)?;
            } else {
                match normalize_path(&p) {
                    Ok(np) => {
                        if !np.is_empty() && !should_ignore(&np, &cfg) { 
                            out.push(np); 
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: skipping invalid path {}: {}", s, e);
                        continue;
                    }
                }
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn split_path(path: &str) -> Vec<&str> { path.split('/').filter(|s| !s.is_empty()).collect() }

fn build_tree_from_index(index: &BTreeMap<String, String>) -> Result<String> {
    #[derive(Default)]
    struct Node { blobs: Vec<(String, String)>, dirs: BTreeMap<String, Node> }

    fn insert(node: &mut Node, parts: &[&str], blob_hash: &str) {
        if parts.is_empty() { return; }
        if parts.len() == 1 {
            node.blobs.push((parts[0].to_string(), blob_hash.to_string()));
        } else {
            let d = node.dirs.entry(parts[0].to_string()).or_default();
            insert(d, &parts[1..], blob_hash);
        }
    }

    fn store_node(node: &Node) -> Result<String> {
        let mut entries: Vec<TreeEntry> = vec![];
        for (name, h) in &node.blobs {
            entries.push(TreeEntry { name: name.clone(), kind: "blob".into(), hash: h.clone(), mode: "100644".into() });
        }
        for (dname, child) in &node.dirs {
            let th = store_node(child)?;
            entries.push(TreeEntry { name: dname.clone(), kind: "tree".into(), hash: th, mode: "040000".into() });
        }
        store_tree(&Tree { entries })
    }

    let mut root = Node::default();
    for (p, h) in index {
        let parts = split_path(p);
        insert(&mut root, &parts, h);
    }
    store_node(&root)
}

fn flatten_tree_cached(cache: &Cache, root: &str) -> Result<Arc<BTreeMap<String, String>>> {
    if let Some(v) = cache.flat_tree.lock().unwrap().get(root).cloned() {
        return Ok(v);
    }
    fn walk(prefix: &str, tree_hash: &str, out: &mut BTreeMap<String, String>) -> Result<()> {
        let t = load_tree(tree_hash)?;
        for e in t.entries {
            let p = if prefix.is_empty() { e.name.clone() } else { format!("{}/{}", prefix, e.name) };
            if e.kind == "blob" {
                out.insert(p, e.hash);
            } else if e.kind == "tree" {
                walk(&p, &e.hash, out)?;
            } else {
                bail!("unknown tree entry kind");
            }
        }
        Ok(())
    }
    let mut map = BTreeMap::new();
    walk("", root, &mut map)?;
    let arc = Arc::new(map);
    cache.flat_tree.lock().unwrap().insert(root.to_string(), arc.clone());
    Ok(arc)
}

fn commit_effective_map(cache: &Cache, c: &Commit) -> Result<Arc<BTreeMap<String, String>>> {
    if let Some(v) = cache.eff_commit.lock().unwrap().get(&c.id).cloned() {
        return Ok(v);
    }
    let base = flatten_tree_cached(cache, &c.base_root)?;
    let mut out = (*base).clone();
    for lh in &c.layers {
        let layer = load_layer(lh)?;
        for ch in layer.changes {
            match ch {
                LayerChange::Put { path, hash } => { out.insert(path, hash); }
                LayerChange::Del { path } => { out.remove(&path); }
            }
        }
    }
    let arc = Arc::new(out);
    cache.eff_commit.lock().unwrap().insert(c.id.clone(), arc.clone());
    Ok(arc)
}

fn resolve_id_or_branch(s: &str) -> Result<String> {
    let heads = PathBuf::from(POP_DIR).join("refs").join("heads").join(s);
    if heads.exists() {
        return Ok(fs::read_to_string(heads)?.lines().next().unwrap_or("").trim().to_string());
    }
    let cp = PathBuf::from(POP_DIR).join("commits").join(format!("{}.json", s));
    if cp.exists() { return Ok(s.to_string()); }
    bail!("unknown id/branch: {}", s)
}

fn lock_local(path: String, ttl: u64) -> Result<()> {
    ensure_repo()?;
    let p = normalize_path(Path::new(&path))?;
    if p.is_empty() { bail!("bad path"); }
    let mut db = read_locks();
    purge_expired(&mut db);
    let me = owner_string();

    if let Some(e) = db.locks.get(&p) {
        if e.owner == me {
            println!("already locked: {} (you)", p);
            return Ok(());
        }
        bail!("already locked: {} by {}", p, e.owner);
    }

    let e = LockEntry { path: p.clone(), owner: me, token: lock_token(), ts: now_unix(), ttl };
    db.locks.insert(p.clone(), e);
    write_locks(&db)?;
    println!("locked {}", p);
    Ok(())
}

fn unlock_local(path: String) -> Result<()> {
    ensure_repo()?;
    let p = normalize_path(Path::new(&path))?;
    let mut db = read_locks();
    purge_expired(&mut db);
    let me = owner_string();

    match db.locks.get(&p) {
        None => { println!("not locked: {}", p); return Ok(()); }
        Some(e) if e.owner != me => bail!("cannot unlock: {} locked by {}", p, e.owner),
        _ => {}
    }

    db.locks.remove(&p);
    write_locks(&db)?;
    println!("unlocked {}", p);
    Ok(())
}

fn list_locks_local() -> Result<()> {
    ensure_repo()?;
    let mut db = read_locks();
    purge_expired(&mut db);
    write_locks(&db).ok();

    if db.locks.is_empty() { println!("(no locks)"); return Ok(()); }
    for e in db.locks.values() {
        println!("{}  owner={}  expires_in={}s", e.path, e.owner, ((e.ts + e.ttl as i64) - now_unix()).max(0));
    }
    Ok(())
}

fn renew_local(path: String, ttl: u64) -> Result<()> {
    ensure_repo()?;
    let p = normalize_path(Path::new(&path))?;
    let mut db = read_locks();
    purge_expired(&mut db);
    let me = owner_string();

    let Some(e) = db.locks.get_mut(&p) else { bail!("not locked: {}", p); };
    if e.owner != me { bail!("cannot renew: locked by {}", e.owner); }
    e.ts = now_unix();
    e.ttl = ttl;
    write_locks(&db)?;
    println!("renewed {}", p);
    Ok(())
}


fn compute_worktree_blobs_fast(files: &[String], statcache: &mut StatCache) -> Result<BTreeMap<String, String>> {
    let cfg = read_config()?;
    let reads: Result<Vec<(String, u64, u64, Vec<u8>)>> = files.par_iter().filter_map(|f| {
        if should_ignore(f, &cfg) { return None; }
        let p = Path::new(f);
        let ms = file_mtime_size(p);
        let (mtime, size) = match ms { Ok(v) => v, Err(e) => return Some(Err(anyhow!(e))) };
        if let Some(ent) = statcache.files.get(f) {
            if ent.mtime == mtime && ent.size == size && !ent.hash.is_empty() {
                return None;
            }
        }
        match fs::read(p) {
            Ok(b) => Some(Ok((f.clone(), mtime, size, b))),
            Err(e) => Some(Err(anyhow!(e))),
        }
    }).collect();
    let reads = reads?;

    let blobs: Result<Vec<(String, IndexEntry)>> = reads.par_iter().map(|(p, mtime, size, b)| {
        let h = store_blob(b)?;
        Ok((p.clone(), IndexEntry{ hash: h, mtime: *mtime, size: *size }))
    }).collect();
    let blobs = blobs?;

    for (p, e) in blobs {
        statcache.files.insert(p, e);
    }

    let mut m = BTreeMap::new();
    for (p, e) in statcache.files.iter() {
        if should_ignore(p, &cfg) { continue; }
        m.insert(p.clone(), e.hash.clone());
    }
    Ok(m)
}

fn diff_maps(a: &BTreeMap<String, String>, b: &BTreeMap<String, String>) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut added = vec![];
    let mut modified = vec![];
    let mut deleted = vec![];
    for (p, ha) in a {
        match b.get(p) {
            None => deleted.push(p.clone()),
            Some(hb) => if ha != hb { modified.push(p.clone()); }
        }
    }
    for p in b.keys() {
        if !a.contains_key(p) { added.push(p.clone()); }
    }
    added.sort(); modified.sort(); deleted.sort();
    (added, modified, deleted)
}

fn print_diff(title: &str, added: &[String], modified: &[String], deleted: &[String]) {
    println!("{}", title);
    if added.is_empty() && modified.is_empty() && deleted.is_empty() {
        println!("  (none)");
        return;
    }
    for f in added { println!("  + {}", f); }
    for f in modified { println!("  ~ {}", f); }
    for f in deleted { println!("  - {}", f); }
}

fn add_paths(paths: Vec<String>) -> Result<()> {
    ensure_repo()?;
    let files = scan_files(&paths)?;
    if files.is_empty() { bail!("no files found"); }
    let cfg = read_config()?;
    let reads: Result<Vec<(String, u64, u64, Vec<u8>)>> = files.par_iter().filter_map(|f| {
        if should_ignore(f, &cfg) { return None; }
        let p = Path::new(f);
        let (mtime, size) = match file_mtime_size(p) { Ok(v) => v, Err(e) => return Some(Err(anyhow!(e))) };
        match fs::read(p) {
            Ok(b) => Some(Ok((f.clone(), mtime, size, b))),
            Err(e) => Some(Err(anyhow!(e))),
        }
    }).collect();
    let reads = reads?;
    let blobs: Result<Vec<(String, IndexEntry)>> = reads.par_iter().map(|(p, mtime, size, b)| {
        let h = store_blob(b)?;
        Ok((p.clone(), IndexEntry { hash: h, size: *size, mtime: *mtime }))
    }).collect();
    let blobs = blobs?;
    let mut idx = read_index()?;
    for (p, e) in blobs {
        idx.del.remove(&p);
        idx.put.insert(p, e);
    }
    write_index(&idx)?;
    Ok(())
}

fn rm_paths(paths: Vec<String>) -> Result<()> {
    ensure_repo()?;
    if paths.is_empty() { bail!("rm requires paths"); }
    let mut idx = read_index()?;
    for p in paths {
        match normalize_path(Path::new(&p)) {
            Ok(np) => {
                if np.is_empty() { continue; }
                idx.put.remove(&np);
                idx.del.insert(np);
            }
            Err(e) => {
                eprintln!("Warning: skipping invalid path {}: {}", p, e);
                continue;
            }
        }
    }
    write_index(&idx)?;
    Ok(())
}

fn current_commit(_cache: &Cache) -> Result<Commit> {
    let id = current_commit_id()?;
    read_commit(&id)
}

fn status_cmd() -> Result<()> {
    ensure_repo()?;
    let cache = Cache::default();
    let head = current_commit(&cache)?;
    let head_files = commit_effective_map(&cache, &head)?;
    let idx = read_index()?;
    let mut staged_target = (*head_files).clone();
    for (p, e) in &idx.put { staged_target.insert(p.clone(), e.hash.clone()); }
    for p in &idx.del { staged_target.remove(p); }
    let (sa, sm, sd) = diff_maps(&head_files, &staged_target);

    let work_files = scan_files(&[])?;
    let mut sc = read_statcache();
    let work_blobs = compute_worktree_blobs_fast(&work_files, &mut sc).unwrap_or_default();
    write_statcache(&sc).ok();
    let (ua, um, ud) = diff_maps(&head_files, &work_blobs);

    let h = read_head()?;
    let head_label = if let Some(d) = &h.detached { format!("detached@{}", d) } else { h.r#ref.clone().unwrap_or_else(|| "HEAD".into()) };

    println!("On: {}", head_label);
    let cfg = read_config()?;
    if cfg.sparse.is_empty() { println!("Sparse: (none)"); } else { println!("Sparse: {}", cfg.sparse.join(", ")); }
    print_diff("\nStaged:", &sa, &sm, &sd);
    print_diff("\nUnstaged:", &ua, &um, &ud);
    Ok(())
}

fn make_layer_from_index(parent: &BTreeMap<String, String>, idx: &Index) -> Result<(Option<String>, bool)> {
    let mut changes: Vec<LayerChange> = vec![];
    for (p, e) in &idx.put {
        if parent.get(p) != Some(&e.hash) {
            changes.push(LayerChange::Put { path: p.clone(), hash: e.hash.clone() });
        }
    }
    for p in &idx.del {
        if parent.contains_key(p) {
            changes.push(LayerChange::Del { path: p.clone() });
        }
    }
    if changes.is_empty() { return Ok((None, false)); }
    let layer = Layer { changes };
    let lh = store_layer(&layer)?;
    Ok((Some(lh), true))
}

fn commit_cmd(message: String) -> Result<()> {
    ensure_repo()?;
    if message.trim().is_empty() { bail!("commit requires -m message"); }

    let cache = Cache::default();
    let old_id = current_commit_id()?;
    let old = read_commit(&old_id)?;
    let parent_map = commit_effective_map(&cache, &old)?;
let idx = read_index()?;
let cfg = read_config()?;
assert_locks_for_index(&idx, &cfg)?;
let (new_layer_opt, changed) = make_layer_from_index(&parent_map, &idx)?;
    if !changed { bail!("nothing to commit"); }

    let mut layers = old.layers.clone();
    if let Some(lh) = new_layer_opt.clone() { layers.push(lh); }

    let mut effective_map = (*parent_map).clone();
    for (p, e) in &idx.put { effective_map.insert(p.clone(), e.hash.clone()); }
    for p in &idx.del { effective_map.remove(p); }
    let mut effective_root = build_tree_from_index(&effective_map)?;

    let mut base_root = old.base_root.clone();
    if layers.len() > LAYER_SQUASH_THRESHOLD {
        base_root = effective_root.clone();
        layers.clear();
        effective_root = base_root.clone();
    }

    let seq = next_seq()?;
    let id = format!("cmt_{:07}", seq);
    let c = Commit {
        id: id.clone(),
        parents: vec![old_id.clone()],
        message,
        seq,
        base_root,
        layers,
        effective_root,
        ts: now_unix(),
    };
    write_commit(&c)?;

    let mut h = read_head()?;
    if h.detached.is_some() {
        h.detached = Some(id.clone());
        write_head(&h)?;
    } else {
        let r = h.r#ref.clone().ok_or_else(|| anyhow!("HEAD invalid"))?;
        write_ref(&r, &id)?;
    }

    write_index(&Index::default())?;
    append_reflog(&old_id, &id, "commit")?;
    Ok(())
}

fn log_cmd(n: Option<usize>) -> Result<()> {
    ensure_repo()?;
    let mut id = current_commit_id()?;
    let mut left = n.unwrap_or(usize::MAX);
    while left > 0 {
        let c = read_commit(&id)?;
        println!("{}  #{}", c.id, c.seq);
        println!("  {}", c.message);
        println!("  ts {}", c.ts);
        println!("  layers {}  base {}", c.layers.len(), c.base_root);
        if c.parents.is_empty() { break; }
        id = c.parents[0].clone();
        left -= 1;
    }
    Ok(())
}

fn branch(name: String) -> Result<()> {
    ensure_repo()?;
    if name.contains('/') || name.contains('\\') { bail!("invalid branch name"); }
    let id = current_commit_id()?;
    let p = PathBuf::from(POP_DIR).join("refs").join("heads").join(&name);
    if p.exists() { bail!("branch already exists"); }
    atomic_write(&p, format!("{}\n", id).as_bytes())?;
    Ok(())
}

fn diff_cmd(from: Option<String>, to: Option<String>) -> Result<()> {
    ensure_repo()?;
    let cache = Cache::default();
    let to_id = match to { Some(v) => resolve_id_or_branch(&v)?, None => current_commit_id()? };
    let to_c = read_commit(&to_id)?;
    let to_files = commit_effective_map(&cache, &to_c)?;

    let from_id = match from {
        Some(v) => resolve_id_or_branch(&v)?,
        None => if to_c.parents.is_empty() { to_id.clone() } else { to_c.parents[0].clone() }
    };
    let from_c = read_commit(&from_id)?;
    let from_files = commit_effective_map(&cache, &from_c)?;

    let (a, m, d) = diff_maps(&from_files, &to_files);
    println!("diff {} -> {}", from_id, to_id);
    print_diff("", &a, &m, &d);
    Ok(())
}

fn is_text(b: &[u8]) -> bool {
    if b.is_empty() { return true; }
    if b.iter().any(|&c| c == 0) { return false; }
    let bad = b.iter().filter(|&&c| c < 9 || (c > 13 && c < 32)).count();
    bad * 100 / b.len() < 2
}

fn conflict_bytes(path: &str, ours: &[u8], theirs: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("<<<<<<< ours ({})\n", path).as_bytes());
    out.extend_from_slice(ours);
    if !ours.ends_with(b"\n") { out.extend_from_slice(b"\n"); }
    out.extend_from_slice(b"=======\n");
    out.extend_from_slice(theirs);
    if !theirs.ends_with(b"\n") { out.extend_from_slice(b"\n"); }
    out.extend_from_slice(format!(">>>>>>> theirs ({})\n", path).as_bytes());
    out
}

fn merge_text_3way(path: &str, base: &[u8], ours: &[u8], theirs: &[u8]) -> Vec<u8> {
    if ours == theirs { return ours.to_vec(); }
    if ours == base { return theirs.to_vec(); }
    if theirs == base { return ours.to_vec(); }
    conflict_bytes(path, ours, theirs)
}

fn three_way_merge(
    base: &BTreeMap<String, String>,
    ours: &BTreeMap<String, String>,
    theirs: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let mut paths = BTreeSet::new();
    for k in base.keys() { paths.insert(k.clone()); }
    for k in ours.keys() { paths.insert(k.clone()); }
    for k in theirs.keys() { paths.insert(k.clone()); }

    let mut out = BTreeMap::new();
    for p in paths {
        let b = base.get(&p);
        let o = ours.get(&p);
        let t = theirs.get(&p);
        match (b, o, t) {
            (None, None, None) => {}
            (None, Some(oh), None) => { out.insert(p, oh.clone()); }
            (None, None, Some(th)) => { out.insert(p, th.clone()); }
            (None, Some(oh), Some(th)) => {
                if oh == th { out.insert(p, oh.clone()); }
                else {
                    let ob = load_blob(oh)?;
                    let tb = load_blob(th)?;
                    let mb = conflict_bytes(&p, &ob, &tb);
                    let mh = store_blob(&mb)?;
                    out.insert(p, mh);
                }
            }
            (Some(bh), Some(oh), Some(th)) => {
                if oh == th { out.insert(p, oh.clone()); }
                else if oh == bh { out.insert(p, th.clone()); }
                else if th == bh { out.insert(p, oh.clone()); }
                else {
                    let bb = load_blob(bh)?;
                    let ob = load_blob(oh)?;
                    let tb = load_blob(th)?;
                    let mb = if is_text(&bb) && is_text(&ob) && is_text(&tb) { merge_text_3way(&p, &bb, &ob, &tb) } else { conflict_bytes(&p, &ob, &tb) };
                    let mh = store_blob(&mb)?;
                    out.insert(p, mh);
                }
            }
            (Some(bh), Some(oh), None) => { if oh != bh { out.insert(p, oh.clone()); } }
            (Some(bh), None, Some(th)) => { if th != bh { out.insert(p, th.clone()); } }
            (Some(_), None, None) => {}
        }
    }
    Ok(out)
}

fn find_common_ancestor(a: &str, b: &str) -> Option<String> {
    let mut seen = BTreeSet::new();
    let mut cur = a.to_string();
    while let Ok(c) = read_commit(&cur) {
        seen.insert(cur.clone());
        if c.parents.is_empty() { break; }
        cur = c.parents[0].clone();
    }
    let mut cur = b.to_string();
    while let Ok(c) = read_commit(&cur) {
        if seen.contains(&cur) { return Some(cur); }
        if c.parents.is_empty() { break; }
        cur = c.parents[0].clone();
    }
    None
}

fn stream_blob_to_file(hash: &str, path: &Path) -> Result<()> {
    let raw = load_raw_object(hash)?;
    if raw.starts_with(b"blob\n") {
        atomic_write(path, &raw[5..])?;
        return Ok(());
    }
    if raw.starts_with(b"big\n") {
        let meta: BigBlob = serde_json::from_slice(&raw[4..])?;
        let tmp = path.with_extension(format!("tmp_{}", now_unix()));
        {
            let mut f = fs::OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
            for ch in meta.chunks {
                let b = load_chunk(&ch)?;
                f.write_all(&b)?;
            }
            f.sync_all().ok();
        }
        fs::rename(&tmp, path).or_else(|_| {
            fs::remove_file(path).ok();
            fs::rename(&tmp, path)
        })?;
        return Ok(());
    }
    bail!("not a blob/big")
}

fn restore_worktree(files: &BTreeMap<String, String>, remote: Option<RemoteSpec>) -> Result<()> {
    let cfg = read_config()?;
    let current = scan_files(&[])?;
    let current_set: BTreeSet<String> = current.into_iter().filter(|f| !should_ignore(f, &cfg)).collect();
    for f in current_set {
        if !files.contains_key(&f) {
            let _ = fs::remove_file(&f);
        }
    }
    for (p, bh) in files {
        if should_ignore(p, &cfg) { continue; }
        ensure_object_present(bh, &remote)?;
        if let Some(parent) = Path::new(p).parent() {
            if parent != Path::new("") && parent != Path::new(".") { fs::create_dir_all(parent)?; }
        }
        stream_blob_to_file(bh, Path::new(p))?;
    }
    Ok(())
}

fn merge_cmd(branch: String) -> Result<()> {
    ensure_repo()?;
    let cache = Cache::default();
    let theirs_id = resolve_id_or_branch(&branch)?;
    let ours_id = current_commit_id()?;
    let ours = read_commit(&ours_id)?;
    let theirs = read_commit(&theirs_id)?;
    let base_id = find_common_ancestor(&ours_id, &theirs_id).unwrap_or_else(|| "cmt_0000000".into());
    let base = read_commit(&base_id)?;
    let base_files = commit_effective_map(&cache, &base)?;
    let ours_files = commit_effective_map(&cache, &ours)?;
    let theirs_files = commit_effective_map(&cache, &theirs)?;
    let merged = three_way_merge(&base_files, &ours_files, &theirs_files)?;
    restore_worktree(&merged, None)?;
    let mut idx = read_index()?;
    idx.put.clear(); idx.del.clear();
    for (p, h) in merged {
        let (mtime, size) = file_mtime_size(Path::new(&p)).unwrap_or((0, 0));
        idx.put.insert(p, IndexEntry{ hash: h, mtime, size });
    }
    write_index(&idx)?;
    Ok(())
}

fn make_patch(from: &BTreeMap<String, String>, to: &BTreeMap<String, String>) -> (BTreeMap<String, String>, BTreeSet<String>) {
    let mut put = BTreeMap::new();
    let mut del = BTreeSet::new();
    for (p, fh) in from {
        match to.get(p) {
            None => { del.insert(p.clone()); }
            Some(th) => if fh != th { put.insert(p.clone(), th.clone()); }
        }
    }
    for (p, th) in to {
        if !from.contains_key(p) { put.insert(p.clone(), th.clone()); }
    }
    (put, del)
}

fn apply_patch(base: &BTreeMap<String, String>, put: &BTreeMap<String, String>, del: &BTreeSet<String>) -> BTreeMap<String, String> {
    let mut out = base.clone();
    for d in del { out.remove(d); }
    for (p, h) in put { out.insert(p.clone(), h.clone()); }
    out
}

fn cherry_pick_cmd(commit: String) -> Result<()> {
    ensure_repo()?;
    let cache = Cache::default();
    let target_id = resolve_id_or_branch(&commit)?;
    let target = read_commit(&target_id)?;
    if target.parents.is_empty() { bail!("cannot cherry-pick root commit"); }
    let parent = read_commit(&target.parents[0])?;
    let parent_files = commit_effective_map(&cache, &parent)?;
    let target_files = commit_effective_map(&cache, &target)?;
    let (put, del) = make_patch(&parent_files, &target_files);
    let head_id = current_commit_id()?;
    let head = read_commit(&head_id)?;
    let head_files = commit_effective_map(&cache, &head)?;
    let new_files = apply_patch(&head_files, &put, &del);
    restore_worktree(&new_files, None)?;
    let mut idx = read_index()?;
    idx.put.clear(); idx.del.clear();
    for (p, h) in new_files {
        let (mtime, size) = file_mtime_size(Path::new(&p)).unwrap_or((0, 0));
        idx.put.insert(p, IndexEntry{ hash: h, mtime, size });
    }
    write_index(&idx)?;
    Ok(())
}

fn rebase_cmd(onto: String) -> Result<()> {
    ensure_repo()?;
    let cache = Cache::default();
    let onto_id = resolve_id_or_branch(&onto)?;
    let onto_c = read_commit(&onto_id)?;
    let head_id = current_commit_id()?;
    let head = read_commit(&head_id)?;
    let base_id = find_common_ancestor(&head_id, &onto_id).unwrap_or_else(|| "cmt_0000000".into());
    if base_id == head_id { return Ok(()); }

    let mut chain: Vec<Commit> = vec![];
    let mut cur = head_id.clone();
    while cur != base_id {
        let c = read_commit(&cur)?;
        chain.push(c.clone());
        if c.parents.is_empty() { break; }
        cur = c.parents[0].clone();
    }
    chain.reverse();

    let mut new_parent_id = onto_id.clone();
    let mut new_parent = onto_c;
    for old_commit in chain {
        let old_parent_id = old_commit.parents.get(0).cloned().unwrap_or_else(|| base_id.clone());
        let old_parent = read_commit(&old_parent_id)?;
        let old_parent_files = commit_effective_map(&cache, &old_parent)?;
        let old_files = commit_effective_map(&cache, &old_commit)?;
        let (put, del) = make_patch(&old_parent_files, &old_files);

        let new_parent_files = commit_effective_map(&cache, &new_parent)?;
        let new_files = apply_patch(&new_parent_files, &put, &del);

        let new_root = build_tree_from_index(&new_files)?;
        let seq = next_seq()?;
        let new_id = format!("cmt_{:07}", seq);
        let new_commit = Commit {
            id: new_id.clone(),
            parents: vec![new_parent_id.clone()],
            message: old_commit.message.clone(),
            seq,
            base_root: new_root.clone(),
            layers: vec![],
            effective_root: new_root,
            ts: now_unix(),
        };
        write_commit(&new_commit)?;
        new_parent_id = new_id.clone();
        new_parent = new_commit;
    }

    let mut h = read_head()?;
    let old = current_commit_id()?;
    if h.detached.is_some() {
        h.detached = Some(new_parent_id.clone());
        write_head(&h)?;
    } else {
        let r = h.r#ref.clone().ok_or_else(|| anyhow!("HEAD invalid"))?;
        write_ref(&r, &new_parent_id)?;
    }
    append_reflog(&old, &new_parent_id, "rebase")?;
    Ok(())
}

fn reflog_cmd() -> Result<()> {
    ensure_repo()?;
    let v = read_reflog()?;
    if v.is_empty() { println!("(empty)"); return Ok(()); }
    for e in v.iter().rev().take(500) {
        println!("ts {}  {} -> {}  {}", e.ts, e.old, e.new, e.action);
    }
    Ok(())
}

fn fsck_cmd() -> Result<()> {
    ensure_repo()?;
    let (_root, _objects, _commits, refs, _head_json, _index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();
    let mut ok = true;

    let mut roots = vec![];
    if refs.exists() {
        for e in fs::read_dir(&refs)? {
            let e = e?;
            if e.file_type()?.is_file() {
                let id = fs::read_to_string(e.path())?.lines().next().unwrap_or("").trim().to_string();
                if !id.is_empty() { roots.push(id); }
            }
        }
    }
    roots.push(current_commit_id().unwrap_or_default());

    let mut seen_commits = BTreeSet::new();
    let mut stack: Vec<String> = roots.into_iter().filter(|s| !s.is_empty()).collect();
    let cache = Cache::default();

    while let Some(id) = stack.pop() {
        if seen_commits.contains(&id) { continue; }
        let c = match read_commit(&id) {
            Ok(v) => v,
            Err(e) => { eprintln!("missing commit {} {}", id, e); ok = false; continue; }
        };
        seen_commits.insert(id.clone());
        for p in &c.parents { stack.push(p.clone()); }

        if load_tree(&c.base_root).is_err() { eprintln!("bad base tree {} in {}", c.base_root, c.id); ok = false; }
        for lh in &c.layers { if load_layer(lh).is_err() { eprintln!("bad layer {} in {}", lh, c.id); ok = false; } }
        if load_tree(&c.effective_root).is_err() { eprintln!("bad effective tree {} in {}", c.effective_root, c.id); ok = false; }

        if let Ok(files) = commit_effective_map(&cache, &c) {
            for (_p, bh) in files.iter() {
                if load_blob(bh).is_err() {
                    eprintln!("bad blob {} referenced by {}", bh, c.id);
                    ok = false;
                }
            }
        } else {
            ok = false;
        }
    }

    if ok { println!("fsck ok"); Ok(()) } else { bail!("fsck failed") }
}

fn gc_cmd() -> Result<()> {
    ensure_repo()?;
    let cache = Cache::default();
    let (_root, objects, _commits, refs, _head_json, _index_json, _cfg_json, _seq_path, _reflog_path, _statcache) = repo_paths();

    let mut roots = vec![];
    if refs.exists() {
        for e in fs::read_dir(&refs)? {
            let e = e?;
            if e.file_type()?.is_file() {
                let id = fs::read_to_string(e.path())?.lines().next().unwrap_or("").trim().to_string();
                if !id.is_empty() { roots.push(id); }
            }
        }
    }
    roots.push(current_commit_id().unwrap_or_default());

    let mut keep_commits = BTreeSet::new();
    let mut keep_objs = BTreeSet::new();
    let mut stack: Vec<String> = roots.into_iter().filter(|s| !s.is_empty()).collect();

    while let Some(id) = stack.pop() {
        if keep_commits.contains(&id) { continue; }
        let c = match read_commit(&id) { Ok(v) => v, Err(_) => continue };
        keep_commits.insert(id.clone());
        for p in &c.parents { stack.push(p.clone()); }

        keep_objs.insert(c.base_root.clone());
        keep_objs.insert(c.effective_root.clone());
        for lh in &c.layers { keep_objs.insert(lh.clone()); }

        if let Ok(files) = commit_effective_map(&cache, &c) {
            for (_p, bh) in files.iter() { keep_objs.insert(bh.clone()); }
        }

        fn walk_tree_collect(hash: &str, keep: &mut BTreeSet<String>) -> Result<()> {
            keep.insert(hash.to_string());
            let t = load_tree(hash)?;
            for e in t.entries {
                keep.insert(e.hash.clone());
                if e.kind == "tree" { walk_tree_collect(&e.hash, keep)?; }
            }
            Ok(())
        }

        let mut tmp = BTreeSet::new();
        let _ = walk_tree_collect(&c.base_root, &mut tmp);
        let _ = walk_tree_collect(&c.effective_root, &mut tmp);
        for h in tmp { keep_objs.insert(h); }
    }

    let mut errors = vec![];
    if objects.exists() {
        for a in fs::read_dir(&objects)? {
            let a = a?;
            if !a.file_type()?.is_dir() { continue; }
            for o in fs::read_dir(a.path())? {
                let o = o?;
                if o.file_type()?.is_file() {
                    let h = o.file_name().to_string_lossy().to_string();
                    if !keep_objs.contains(&h) {
                        if let Err(e) = fs::remove_file(o.path()) {
                            errors.push(format!("failed to delete {}: {}", h, e));
                        }
                    }
                }
            }
        }
    }
    if !errors.is_empty() {
        eprintln!("gc warnings: {}", errors.join(", "));
    }
    Ok(())
}

fn ignore_add(pattern: String) -> Result<()> {
    ensure_repo()?;
    let mut cfg = read_config()?;
    cfg.ignore.push(pattern);
    write_config(&cfg)
}

fn ignore_show() -> Result<()> {
    ensure_repo()?;
    let cfg = read_config()?;
    if cfg.ignore.is_empty() { println!("(no ignore patterns)"); return Ok(()); }
    for p in cfg.ignore { println!("{}", p); }
    Ok(())
}


fn sparse_set(paths: Vec<String>) -> Result<()> {
    ensure_repo()?;
    if paths.is_empty() { bail!("usage: pop sparse set <path...>"); }
    let mut cfg = read_config()?;
    cfg.sparse = paths.into_iter().map(|p| {
        let p = p.replace('\\', "/");
        if p.ends_with('/') { format!("{}**", p) } else { format!("{}/**", p) }
    }).collect();
    write_config(&cfg)
}

fn sparse_clear() -> Result<()> {
    ensure_repo()?;
    let mut cfg = read_config()?;
    cfg.sparse.clear();
    write_config(&cfg)
}
fn sparse_show() -> Result<()> {
    ensure_repo()?;
    let cfg = read_config()?;
    if cfg.sparse.is_empty() { println!("(no sparse patterns)"); return Ok(()); }
    for p in cfg.sparse { println!("{}", p); }
    Ok(())
}

#[derive(Clone, Debug)]
enum RemoteSpec {
    Http { base: String, repo: String, token: Option<String> },
    Ssh  { user_host: String, root: String, repo: String },
}

fn parse_remote(url: &str, repo: &str, token: &Option<String>, ssh_root: &Option<String>) -> Result<RemoteSpec> {
    if url.starts_with("ssh://") {
        let rest = url.trim_start_matches("ssh://");
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 { bail!("bad ssh url, expected ssh://user@host:/abs/root"); }
        let user_host = parts[0].to_string();
        let mut root = parts[1].to_string();
        if root.starts_with("//") { root = root.trim_start_matches('/').to_string(); root = format!("/{}", root); }
        if let Some(r) = ssh_root { root = r.clone(); }
        if !root.starts_with('/') { bail!("ssh root must be absolute linux path, got {}", root); }
        Ok(RemoteSpec::Ssh { user_host, root, repo: repo.to_string() })
    } else {
        Ok(RemoteSpec::Http { base: url.trim_end_matches('/').to_string(), repo: repo.to_string(), token: token.clone() })
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    let mut r: u8 = 0;
    for i in 0..a.len() { r |= a[i] ^ b[i]; }
    r == 0
}

fn has_cmd(cmd: &str) -> bool {
    Command::new(cmd).arg("-V").stdout(Stdio::null()).stderr(Stdio::null()).status().is_ok()
}

fn ssh_run(user_host: &str, script: &str) -> Result<(i32, String, String)> {
    let mut c = Command::new("ssh");
    c.arg(user_host)
        .arg("sh")
        .arg("-lc")
        .arg(script)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = c.output().context("run ssh")?;
    let code = out.status.code().unwrap_or(1);
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    Ok((code, stdout, stderr))
}

fn ssh_has_python(user_host: &str) -> Result<bool> {
    let cmd = "command -v python3 >/dev/null 2>&1 || command -v python >/dev/null 2>&1";
    let (code, _, _) = ssh_run(user_host, cmd)?;
    Ok(code == 0)
}

fn ssh_put_file_b64(user_host: &str, remote_path: &str, bytes: &[u8]) -> Result<()> {
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    let script = format!(r#"
set -e
p={p}
mkdir -p "$(dirname "$p")"
python3 - <<'PY' || python - <<'PY'
import base64, os
p = {p}
b = base64.b64decode({b}.encode("ascii"))
with open(p, "wb") as f:
    f.write(b)
PY
"#, p=sh_quote(remote_path), b=sh_quote(&b64));
    let (code, _, se) = ssh_run(user_host, &script)?;
    if code != 0 { bail!("ssh put failed: {}", se.trim()); }
    Ok(())
}

fn ssh_get_file_b64(user_host: &str, remote_path: &str) -> Result<Vec<u8>> {
    let script = format!(r#"
set -e
p={p}
python3 - <<'PY' || python - <<'PY'
import base64, sys
p = {p}
with open(p, "rb") as f:
    sys.stdout.write(base64.b64encode(f.read()).decode("ascii"))
PY
"#, p=sh_quote(remote_path));
    let (code, so, se) = ssh_run(user_host, &script)?;
    if code != 0 { bail!("ssh get failed: {}", se.trim()); }
    let bytes = base64::engine::general_purpose::STANDARD.decode(so.trim()).map_err(|e| anyhow!(e))?;
    Ok(bytes)
}

fn sh_quote(s: &str) -> String {
    let mut out = String::from("'");
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\"'\"'");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn ensure_object_present(hash: &str, remote: &Option<RemoteSpec>) -> Result<()> {
    let p = obj_path(hash);
    if p.exists() { return Ok(()); }
    let Some(r) = remote else { bail!("missing object: {}", hash); };

    match r {
        RemoteSpec::Http { base, repo, token } => {
            if has_cmd("curl") {
                let url = format!("{}/r/{}/obj/{}", base.trim_end_matches('/'), repo, hash);
                let mut cmd = Command::new("curl");
                cmd.arg("-sS").arg(url);
                if let Some(t) = token {
                    cmd.arg("-H").arg(format!("authorization: Bearer {}", t));
                }
                let out = cmd.output().context("curl fetch obj")?;
                if !out.status.success() { bail!("curl failed fetching object"); }
                let resp: ObjResp = serde_json::from_slice(&out.stdout)?;
                if !resp.ok { bail!("remote object not ok"); }
                let c = base64::engine::general_purpose::STANDARD.decode(resp.bytes_b64).map_err(|e| anyhow!(e))?;
                if let Some(par) = p.parent() { fs::create_dir_all(par)?; }
                atomic_write(&p, &c)?;
                Ok(())
            } else {
                bail!("missing object {} and no curl available", hash)
            }
        }
        RemoteSpec::Ssh { user_host, root, repo } => {
            let rp = format!("{}/{}/{}", root.trim_end_matches('/'), repo, POP_DIR);
            let remote_obj = format!("{}/objects/{}/{}", rp, &hash[0..2], hash);
            let c = ssh_get_file_b64(user_host, &remote_obj)?;
            if let Some(par) = p.parent() { fs::create_dir_all(par)?; }
            atomic_write(&p, &c)?;
            Ok(())
        }
    }
}

fn ensure_repo_dir_local(root: &Path) -> Result<()> {
    fs::create_dir_all(root.join(POP_DIR).join("objects"))?;
    fs::create_dir_all(root.join(POP_DIR).join("commits"))?;
    fs::create_dir_all(root.join(POP_DIR).join("refs").join("heads"))?;
    Ok(())
}

fn make_bundle_from_repo(root: &Path) -> Result<RemoteBundle> {
    let repo = root.join(POP_DIR);
    if !repo.exists() {
        ensure_repo_dir_local(root)?;
        let cfg = Config::default();
        write_json(&repo.join("config.json"), &cfg)?;
        write_json(&repo.join("HEAD.json"), &Head::default())?;
        write_json(&repo.join("reflog.json"), &Vec::<ReflogEntry>::new())?;
        atomic_write(&repo.join("SEQ"), b"0\n")?;
        return Ok(RemoteBundle{commits: vec![], refs: BTreeMap::new(), head: Head::default(), config: cfg, reflog: vec![], seq: 0});
    }

    let cfg: Config = read_json(&repo.join("config.json")).unwrap_or_default();
    let head: Head = read_json(&repo.join("HEAD.json")).unwrap_or_default();
    let reflog: Vec<ReflogEntry> = read_json(&repo.join("reflog.json")).unwrap_or_default();
    let seq: u64 = fs::read_to_string(repo.join("SEQ")).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0);

    let mut refs = BTreeMap::new();
    let heads = repo.join("refs").join("heads");
    if heads.exists() {
        for e in fs::read_dir(&heads)? {
            let e = e?;
            let name = e.file_name().to_string_lossy().to_string();
            let id = fs::read_to_string(e.path())?.lines().next().unwrap_or("").trim().to_string();
            refs.insert(format!("refs/heads/{}", name), id);
        }
    }

    let mut commits = vec![];
    let commits_dir = repo.join("commits");
    if commits_dir.exists() {
        for e in fs::read_dir(&commits_dir)? {
            let e = e?;
            if e.file_type()?.is_file() {
                let c: Commit = read_json(&e.path())?;
                commits.push(c);
            }
        }
    }

    Ok(RemoteBundle{commits, refs, head, config: cfg, reflog, seq})
}

fn apply_bundle_local(root: &Path, bundle: &RemoteBundle) -> Result<()> {
    ensure_repo_dir_local(root)?;
    let repo = root.join(POP_DIR);

    for c in &bundle.commits {
        write_json(&repo.join("commits").join(format!("{}.json", c.id)), c)?;
    }
    for (r, id) in &bundle.refs {
        let rp = repo.join(r.replace('/', &std::path::MAIN_SEPARATOR.to_string()));
        if let Some(par) = rp.parent() { fs::create_dir_all(par)?; }
        atomic_write(&rp, format!("{}\n", id).as_bytes())?;
    }
    write_json(&repo.join("HEAD.json"), &bundle.head)?;
    write_json(&repo.join("config.json"), &bundle.config)?;
    write_json(&repo.join("reflog.json"), &bundle.reflog)?;
    atomic_write(&repo.join("SEQ"), format!("{}\n", bundle.seq).as_bytes())?;
    Ok(())
}

fn collect_needed_objects_for_bundle(bundle: &RemoteBundle, local_root: &Path) -> Result<BTreeSet<String>> {
    let cache = Cache::default();
    let mut objs = BTreeSet::new();

    for c in &bundle.commits {
        objs.insert(c.base_root.clone());
        objs.insert(c.effective_root.clone());
        for lh in &c.layers { objs.insert(lh.clone()); }

        let files = commit_effective_map(&cache, c)?;
        for (_p, bh) in files.iter() { objs.insert(bh.clone()); }

        fn walk_tree(hash: &str, objs: &mut BTreeSet<String>) -> Result<()> {
            objs.insert(hash.to_string());
            let t = load_tree(hash)?;
            for e in t.entries {
                objs.insert(e.hash.clone());
                if e.kind == "tree" { walk_tree(&e.hash, objs)?; }
            }
            Ok(())
        }
        let _ = walk_tree(&c.base_root, &mut objs);
        let _ = walk_tree(&c.effective_root, &mut objs);
    }

    for h in objs.clone() {
        if h.len() < 2 { continue; }
        let p = local_root.join(POP_DIR).join("objects").join(&h[0..2]).join(&h);
        if !p.exists() { return Ok(objs); }
    }
    Ok(objs)
}

fn build_python_apply_script(rp_json: &str) -> String {
    let tpl = r#"
set -e
python3 - <<'PY' || python - <<'PY'
import json, os
rp = __RP__
with open(os.path.join(rp, "bundle.json"), "r", encoding="utf-8") as f:
    b = json.load(f)

def wj(p, v):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(v, f, indent=2, ensure_ascii=False)

for c in b.get("commits", []):
    wj(os.path.join(rp, "commits", f"{c['id']}.json"), c)

for ref, cid in b.get("refs", {}).items():
    p = os.path.join(rp, *ref.split("/"))
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write(cid + "\n")

wj(os.path.join(rp, "HEAD.json"), b.get("head", {}))
wj(os.path.join(rp, "config.json"), b.get("config", {}))
wj(os.path.join(rp, "reflog.json"), b.get("reflog", []))

with open(os.path.join(rp, "SEQ"), "w", encoding="utf-8") as f:
    f.write(str(b.get("seq", 0)) + "\n")
PY
"#;
    tpl.replace("__RP__", rp_json)
}


fn push_remote_cmd(a: RemoteArgs) -> Result<()> {
    ensure_repo()?;
    let root = Path::new(".");
    let bundle = make_bundle_from_repo(root)?;
    let need = collect_needed_objects_for_bundle(&bundle, root)?;
    let remote = parse_remote(&a.url, &a.repo, &a.token, &a.ssh_root)?;

    match remote {
        RemoteSpec::Http { base, repo, token } => {
            if has_cmd("curl") {
                for h in need {
                    if h.len() < 2 { continue; }
                    let p = root.join(POP_DIR).join("objects").join(&h[0..2]).join(&h);
                    if !p.exists() { continue; }
                    let bytes = fs::read(p)?;
                    let enc = base64::engine::general_purpose::STANDARD.encode(bytes);
                    let obj_url = format!("{}/r/{}/obj/{}", base, repo, h);
                    let json_body = serde_json::to_string(&PutBytesReq{ bytes_b64: enc })?;
                    let mut cmd = Command::new("curl");
                    cmd.arg("-sS").arg("-X").arg("POST").arg(obj_url).arg("-H").arg("content-type: application/json").arg("--data-binary").arg(json_body);
                    if let Some(t) = &token { cmd.arg("-H").arg(format!("authorization: Bearer {}", t)); }
                    let out = cmd.output().context("curl put obj")?;
                    if !out.status.success() { bail!("curl put obj failed"); }
                }
                let push_url = format!("{}/r/{}/push", base, repo);
                let json_body = serde_json::to_string(&PushReq{ repo: repo.clone(), bundle })?;
                let mut cmd = Command::new("curl");
                cmd.arg("-sS").arg("-X").arg("POST").arg(push_url).arg("-H").arg("content-type: application/json").arg("--data-binary").arg(json_body);
                if let Some(t) = &token { cmd.arg("-H").arg(format!("authorization: Bearer {}", t)); }
                let out = cmd.output().context("curl push")?;
                if !out.status.success() { bail!("curl push failed"); }
                Ok(())
            } else {
                bail!("HTTP push requires curl on PATH.")
            }
        }
        RemoteSpec::Ssh { user_host, root, repo } => {
            if !has_cmd("ssh") { bail!("ssh not found on PATH."); }
            if !ssh_has_python(&user_host)? { bail!("ssh server must have python3 or python."); }
            let rp = format!("{}/{}/{}", root.trim_end_matches('/'), repo, POP_DIR);
            let (code, _, se) = ssh_run(&user_host, &format!(r#"set -e; mkdir -p "{rp}/objects" "{rp}/commits" "{rp}/refs/heads""#, rp=rp))?;
            if code != 0 { bail!("ssh mkdir failed: {}", se.trim()); }

            for h in need {
                if h.len() < 2 { continue; }
                let p = Path::new(".").join(POP_DIR).join("objects").join(&h[0..2]).join(&h);
                if !p.exists() { continue; }
                let bytes = fs::read(&p)?;
                let remote_obj = format!("{}/objects/{}/{}", rp, &h[0..2], h);
                ssh_put_file_b64(&user_host, &remote_obj, &bytes)?;
            }

            let bundle_bytes = serde_json::to_vec_pretty(&bundle)?;
            let remote_bundle = format!("{}/bundle.json", rp);
            ssh_put_file_b64(&user_host, &remote_bundle, &bundle_bytes)?;
let apply_script = build_python_apply_script(&sh_quote(&rp));
let (code2, _, se2) = ssh_run(&user_host, &apply_script)?;
if code2 != 0 { bail!("ssh apply bundle failed: {}", se2.trim()); }
            Ok(())
        }
    }
}

fn pull_remote_cmd(a: RemoteArgs) -> Result<()> {
    ensure_repo()?;
    let remote = parse_remote(&a.url, &a.repo, &a.token, &a.ssh_root)?;
    match remote {
        RemoteSpec::Http { base, repo, token } => {
            if !has_cmd("curl") { bail!("HTTP pull requires curl on PATH."); }
            let url = format!("{}/r/{}/pull", base, repo);
            let mut cmd = Command::new("curl");
            cmd.arg("-sS").arg(url);
            if let Some(t) = &token { cmd.arg("-H").arg(format!("authorization: Bearer {}", t)); }
            let out = cmd.output().context("curl pull")?;
            if !out.status.success() { bail!("curl pull failed"); }
            let pr: PullResp = serde_json::from_slice(&out.stdout)?;
            if !pr.ok { bail!("pull response not ok"); }
            apply_bundle_local(Path::new("."), &pr.bundle)?;
            Ok(())
        }
        RemoteSpec::Ssh { user_host, root, repo } => {
            if !has_cmd("ssh") { bail!("ssh not found on PATH."); }
            if !ssh_has_python(&user_host)? { bail!("ssh server must have python3 or python."); }
            let rp = format!("{}/{}/{}", root.trim_end_matches('/'), repo, POP_DIR);

            let script = format!(r#"
set -e
python3 - <<'PY' || python - <<'PY'
import os, json, base64, sys
rp = {rp}
def rj(p, default):
    try:
        with open(p, "r", encoding="utf-8") as f: return json.load(f)
    except: return default
bundle = {{"commits": [], "refs": {{}}, "head": rj(os.path.join(rp,"HEAD.json"),{{}}), "config": rj(os.path.join(rp,"config.json"),{{}}), "reflog": rj(os.path.join(rp,"reflog.json"),[]), "seq": 0}}
try:
    with open(os.path.join(rp,"SEQ"), "r", encoding="utf-8") as f:
        bundle["seq"] = int((f.read().strip() or "0"))
except: pass
heads = os.path.join(rp,"refs","heads")
if os.path.isdir(heads):
    for name in os.listdir(heads):
        p = os.path.join(heads,name)
        if os.path.isfile(p):
            with open(p,"r",encoding="utf-8") as f:
                bundle["refs"][f"refs/heads/{{name}}"] = (f.read().splitlines()[:1] or [""])[0].strip()
cd = os.path.join(rp,"commits")
if os.path.isdir(cd):
    for fn in os.listdir(cd):
        if fn.endswith(".json"):
            bundle["commits"].append(rj(os.path.join(cd,fn),{{}}))
data = json.dumps({{"ok": True, "bundle": bundle}}, indent=2, ensure_ascii=False).encode("utf-8")
sys.stdout.write(base64.b64encode(data).decode("ascii"))
PY
"#, rp=sh_quote(&rp));
            let (code, so, se) = ssh_run(&user_host, &script)?;
            if code != 0 { bail!("ssh pull failed: {}", se.trim()); }
            let bytes = base64::engine::general_purpose::STANDARD.decode(so.trim()).map_err(|e| anyhow!(e))?;
            let pr: PullResp = serde_json::from_slice(&bytes)?;
            if !pr.ok { bail!("pull response not ok"); }
            apply_bundle_local(Path::new("."), &pr.bundle)?;
            Ok(())
        }
    }
}

#[cfg(feature="server")]
fn auth_ok(headers: &HeaderMap, token: &Option<String>) -> bool {
    let Some(expected) = token else { return false; };
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    ct_eq(got.as_bytes(), expected.as_bytes())
}

#[cfg(feature="server")]
fn repo_root(st: &SrvState, repo: &str) -> Result<PathBuf> {
    if repo.contains("..") || repo.contains('/') || repo.contains('\\') {
        bail!("invalid repo name");
    }
    Ok(st.root.join(repo))
}

#[cfg(feature="server")]
async fn list_repos(State(st): State<SrvState>, headers: HeaderMap) -> (StatusCode, Json<InfoResp>) {
    if !auth_ok(&headers, &st.token) { return (StatusCode::UNAUTHORIZED, Json(InfoResp{ok:false, repos: vec![]})); }
    let mut repos = vec![];
    if let Ok(rd) = fs::read_dir(&st.root) {
        for e in rd.flatten() {
            if e.file_type().ok().map(|x| x.is_dir()).unwrap_or(false) {
                repos.push(e.file_name().to_string_lossy().to_string());
            }
        }
    }
    repos.sort();
    (StatusCode::OK, Json(InfoResp{ok:true, repos}))
}

#[cfg(feature="server")]
async fn pull_repo(State(st): State<SrvState>, headers: HeaderMap, AxPath(repo): AxPath<String>) -> (StatusCode, Json<PullResp>) {
    if !auth_ok(&headers, &st.token) {
        return (StatusCode::UNAUTHORIZED, Json(PullResp{ok:false, bundle: RemoteBundle{commits: vec![], refs: BTreeMap::new(), head: Head::default(), config: Config::default(), reflog: vec![], seq: 0}}));
    }
    let root = match repo_root(&st, &repo) {
        Ok(p) => p,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(PullResp{ok:false, bundle: RemoteBundle{commits: vec![], refs: BTreeMap::new(), head: Head::default(), config: Config::default(), reflog: vec![], seq: 0}})),
    };
    let bundle = match make_bundle_from_repo(&root) {
        Ok(b) => b,
        Err(e) => { warn!("pull bundle error: {}", e); RemoteBundle{commits: vec![], refs: BTreeMap::new(), head: Head::default(), config: Config::default(), reflog: vec![], seq: 0} }
    };
    (StatusCode::OK, Json(PullResp{ok:true, bundle}))
}

#[cfg(feature="server")]
async fn push_repo(State(st): State<SrvState>, headers: HeaderMap, AxPath(repo): AxPath<String>, Json(req): Json<PushReq>) -> (StatusCode, Json<OkResp>) {
    if !auth_ok(&headers, &st.token) { return (StatusCode::UNAUTHORIZED, Json(OkResp{ok:false})); }
    if req.repo != repo { return (StatusCode::BAD_REQUEST, Json(OkResp{ok:false})); }
    let root = match repo_root(&st, &repo) {
        Ok(p) => p,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(OkResp{ok:false})),
    };
    match apply_bundle_local(&root, &req.bundle) {
        Ok(_) => (StatusCode::OK, Json(OkResp{ok:true})),
        Err(e) => { warn!("push apply error: {}", e); (StatusCode::INTERNAL_SERVER_ERROR, Json(OkResp{ok:false})) }
    }
}

#[cfg(feature="server")]
async fn get_obj(State(st): State<SrvState>, headers: HeaderMap, AxPath((repo, hash)): AxPath<(String, String)>) -> (StatusCode, Json<ObjResp>) {
    if !auth_ok(&headers, &st.token) { return (StatusCode::UNAUTHORIZED, Json(ObjResp{ok:false, bytes_b64:"".into()})); }
    if hash.len() < 2 { return (StatusCode::BAD_REQUEST, Json(ObjResp{ok:false, bytes_b64:"".into()})); }
    let root = match repo_root(&st, &repo) {
        Ok(p) => p,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(ObjResp{ok:false, bytes_b64:"".into()})),
    };
    let p = root.join(POP_DIR).join("objects").join(&hash[0..2]).join(&hash);
    if !p.exists() { return (StatusCode::NOT_FOUND, Json(ObjResp{ok:false, bytes_b64:"".into()})); }
    match fs::read(p) {
        Ok(c) => {
            let enc = base64::engine::general_purpose::STANDARD.encode(c);
            (StatusCode::OK, Json(ObjResp{ok:true, bytes_b64: enc}))
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ObjResp{ok:false, bytes_b64:"".into()})),
    }
}

#[cfg(feature="server")]
async fn put_obj(
    State(st): State<SrvState>,
    headers: HeaderMap,
    AxPath((repo, hash)): AxPath<(String, String)>,
    Json(req): Json<PutBytesReq>,
) -> (StatusCode, Json<OkResp>) {
    if !auth_ok(&headers, &st.token) { return (StatusCode::UNAUTHORIZED, Json(OkResp{ok:false})); }
    if hash.len() < 2 { return (StatusCode::BAD_REQUEST, Json(OkResp{ok:false})); }
    let root = match repo_root(&st, &repo) {
        Ok(p) => p,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(OkResp{ok:false})),
    };
    let p = root.join(POP_DIR).join("objects").join(&hash[0..2]).join(&hash);
    if p.exists() { return (StatusCode::OK, Json(OkResp{ok:true})); }
    let bytes = match base64::engine::general_purpose::STANDARD.decode(req.bytes_b64) {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(OkResp{ok:false})),
    };
    if let Some(par) = p.parent() { let _ = fs::create_dir_all(par); }
    match atomic_write(&p, &bytes) {
        Ok(_) => (StatusCode::OK, Json(OkResp{ok:true})),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(OkResp{ok:false})),
    }
}

#[cfg(feature = "server")]
fn normalize_bind_addr(input: &str) -> String {
    let s = input.trim();
    if s.chars().all(|c| c.is_ascii_digit()) {
        return format!("0.0.0.0:{}", s);
    }
    if s.starts_with(':') && s[1..].chars().all(|c| c.is_ascii_digit()) {
        return format!("0.0.0.0{}", s);
    }
    s.to_string()
}

#[cfg(feature = "server")]
use tower::ServiceBuilder;
#[cfg(feature = "server")]
use tower_http::limit::RequestBodyLimitLayer;
#[cfg(feature = "server")]
use std::time::Duration as StdDuration;

#[cfg(feature="server")]
use std::time::Instant;

#[cfg(feature="server")]
use tower_http::timeout::TimeoutLayer;


#[cfg(feature="server")]
#[derive(Clone)]
struct SrvState {
    root: PathBuf,
    token: Option<String>,
    rate_limiter: Arc<Mutex<HashMap<String, (Instant, u32)>>>,
}

#[cfg(feature="server")]
fn repo_locks_path(st: &SrvState, repo: &str) -> Result<PathBuf> {
    if repo.contains("..") || repo.contains('/') || repo.contains('\\') {
        bail!("invalid repo name");
    }
    Ok(st.root.join(repo).join(POP_DIR).join("locks.json"))
}

#[cfg(feature="server")]
fn read_repo_locks(st: &SrvState, repo: &str) -> LockDb {
    repo_locks_path(st, repo)
        .ok()
        .and_then(|p| read_json(&p).ok())
        .unwrap_or_default()
}

#[cfg(feature="server")]
fn write_repo_locks(st: &SrvState, repo: &str, db: &LockDb) -> Result<()> {
    let p = repo_locks_path(st, repo)?;
    write_json(&p, db)
}

#[cfg(feature="server")]
async fn list_locks_repo(
    State(st): State<SrvState>,
    headers: HeaderMap,
    AxPath(repo): AxPath<String>,
) -> (StatusCode, Json<LockResp>) {
    if !auth_ok(&headers, &st.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(LockResp {
                ok: false,
                message: "unauthorized".into(),
                locks: vec![],
            }),
        );
    }

    let mut db = read_repo_locks(&st, &repo);
    purge_expired(&mut db);
    let _ = write_repo_locks(&st, &repo, &db);

    (
        StatusCode::OK,
        Json(LockResp {
            ok: true,
            message: "ok".into(),
            locks: db.locks.values().cloned().collect(),
        }),
    )
}

#[cfg(feature="server")]
async fn lock_repo(
    State(st): State<SrvState>,
    headers: HeaderMap,
    AxPath(repo): AxPath<String>,
    Json(req): Json<LockReq>,
) -> (StatusCode, Json<LockResp>) {
    if !auth_ok(&headers, &st.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(LockResp {
                ok: false,
                message: "unauthorized".into(),
                locks: vec![],
            }),
        );
    }

    let mut db = read_repo_locks(&st, &repo);
    purge_expired(&mut db);

    if let Some(e) = db.locks.get(&req.path) {
        if e.owner != req.owner {
            return (
                StatusCode::CONFLICT,
                Json(LockResp {
                    ok: false,
                    message: format!("already locked by {}", e.owner),
                    locks: db.locks.values().cloned().collect(),
                }),
            );
        }
    }

    let entry = LockEntry {
        path: req.path.clone(),
        owner: req.owner.clone(),
        token: req.token.clone(),
        ts: now_unix(),
        ttl: req.ttl,
    };

    db.locks.insert(req.path.clone(), entry);
    let _ = write_repo_locks(&st, &repo, &db);

    (
        StatusCode::OK,
        Json(LockResp {
            ok: true,
            message: "locked".into(),
            locks: db.locks.values().cloned().collect(),
        }),
    )
}

#[cfg(feature="server")]
async fn unlock_repo(
    State(st): State<SrvState>,
    headers: HeaderMap,
    AxPath(repo): AxPath<String>,
    Json(req): Json<LockReq>,
) -> (StatusCode, Json<LockResp>) {
    if !auth_ok(&headers, &st.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(LockResp {
                ok: false,
                message: "unauthorized".into(),
                locks: vec![],
            }),
        );
    }

    let mut db = read_repo_locks(&st, &repo);
    purge_expired(&mut db);

    match db.locks.get(&req.path) {
        None => {}
        Some(e) => {
            if e.owner != req.owner || !ct_eq(e.token.as_bytes(), req.token.as_bytes()) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(LockResp {
                        ok: false,
                        message: "bad owner or token".into(),
                        locks: db.locks.values().cloned().collect(),
                    }),
                );
            }
        }
    }

    db.locks.remove(&req.path);
    let _ = write_repo_locks(&st, &repo, &db);

    (
        StatusCode::OK,
        Json(LockResp {
            ok: true,
            message: "unlocked".into(),
            locks: db.locks.values().cloned().collect(),
        }),
    )
}

#[cfg(feature="server")]
async fn renew_repo(
    State(st): State<SrvState>,
    headers: HeaderMap,
    AxPath(repo): AxPath<String>,
    Json(req): Json<LockReq>,
) -> (StatusCode, Json<LockResp>) {
    if !auth_ok(&headers, &st.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(LockResp {
                ok: false,
                message: "unauthorized".into(),
                locks: vec![],
            }),
        );
    }

    let mut db = read_repo_locks(&st, &repo);
    purge_expired(&mut db);

    let Some(e) = db.locks.get_mut(&req.path) else {
        return (
            StatusCode::NOT_FOUND,
            Json(LockResp {
                ok: false,
                message: "not locked".into(),
                locks: db.locks.values().cloned().collect(),
            }),
        );
    };

    if e.owner != req.owner || !ct_eq(e.token.as_bytes(), req.token.as_bytes()) {
        return (
            StatusCode::FORBIDDEN,
            Json(LockResp {
                ok: false,
                message: "bad owner or token".into(),
                locks: db.locks.values().cloned().collect(),
            }),
        );
    }

    e.ts = now_unix();
    e.ttl = req.ttl;
    let _ = write_repo_locks(&st, &repo, &db);

    (
        StatusCode::OK,
        Json(LockResp {
            ok: true,
            message: "renewed".into(),
            locks: db.locks.values().cloned().collect(),
        }),
    )
}

#[cfg(feature="server")]
async fn serve_cmd(a: ServeArgs) -> Result<()> {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let addr = if let Ok(v) = std::env::var("POP_ADDR") {
        normalize_bind_addr(&v)
    } else if let Ok(port) = std::env::var("PORT") {
        let p = port.trim();
        if p.is_empty() { normalize_bind_addr(&a.addr) } else { normalize_bind_addr(p) }
    } else {
        normalize_bind_addr(&a.addr)
    };

    let st = SrvState { 
        root: PathBuf::from(a.dir), 
        token: a.token.clone(),
        rate_limiter: Arc::new(Mutex::new(HashMap::new())),
    };
    fs::create_dir_all(&st.root)?;

    let app = Router::new()
        .route("/repos", get(list_repos))
        .route("/r/:repo/pull", get(pull_repo))
        .route("/r/:repo/push", post(push_repo))
        .route("/r/:repo/obj/:hash", get(get_obj).post(put_obj))
        .route("/r/:repo/locks", get(list_locks_repo))
        .route("/r/:repo/lock", post(lock_repo))
        .route("/r/:repo/unlock", post(unlock_repo))
        .route("/r/:repo/renew", post(renew_repo))
.layer(
    ServiceBuilder::new()
        .layer(RequestBodyLimitLayer::new(100 * 1024 * 1024))
        .layer(TimeoutLayer::new(StdDuration::from_secs(300)))
)

        .with_state(st);

    let listener = TcpListener::bind(&addr).await?;
    info!("serving {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(feature="server")]
fn check_rate_limit(st: &SrvState, key: &str, max_per_minute: u32) -> bool {
    let mut limiter = st.rate_limiter.lock().unwrap();
    let now = std::time::Instant::now();

    limiter.retain(|_, (time, _)| now.duration_since(*time) < StdDuration::from_secs(60));

    let entry = limiter.entry(key.to_string()).or_insert((now, 0));

    if now.duration_since(entry.0) > StdDuration::from_secs(60) {
        *entry = (now, 1);
        true
    } else if entry.1 < max_per_minute {
        entry.1 += 1;
        true
    } else {
        false
    }
}

fn checkout_cmd(target: String, remote: Option<String>, repo: Option<String>, token: Option<String>, ssh_root: Option<String>) -> Result<()> {
    ensure_repo()?;
    let mut remote_spec: Option<RemoteSpec> = None;

    if let Some(url) = remote.clone() {
        let repo_name = repo.ok_or_else(|| anyhow!("checkout with --remote requires --repo"))?;
        let r = parse_remote(&url, &repo_name, &token, &ssh_root)?;
        remote_spec = Some(r.clone());

        match r {
            RemoteSpec::Http { base, repo, token } => {
                if !has_cmd("curl") { bail!("HTTP checkout requires curl on PATH."); }
                let pull_url = format!("{}/r/{}/pull", base.trim_end_matches('/'), repo);
                let mut cmd = Command::new("curl");
                cmd.arg("-sS").arg(pull_url);
                if let Some(t) = token { cmd.arg("-H").arg(format!("authorization: Bearer {}", t)); }
                let out = cmd.output().context("curl pull")?;
                if !out.status.success() { bail!("curl pull failed"); }
                let pr: PullResp = serde_json::from_slice(&out.stdout)?;
                if !pr.ok { bail!("pull response not ok"); }
                apply_bundle_local(Path::new("."), &pr.bundle)?;
            }
            RemoteSpec::Ssh { user_host, root, repo } => {
                if !ssh_has_python(&user_host)? { bail!("ssh checkout requires python on server."); }
                let rp = format!("{}/{}/{}", root.trim_end_matches('/'), repo, POP_DIR);

                let script = format!(r#"
set -e
python3 - <<'PY' || python - <<'PY'
import os, json, base64, sys
rp = {rp}
def rj(p, default):
    try:
        with open(p, "r", encoding="utf-8") as f: return json.load(f)
    except: return default
bundle = {{"commits": [], "refs": {{}}, "head": rj(os.path.join(rp,"HEAD.json"),{{}}), "config": rj(os.path.join(rp,"config.json"),{{}}), "reflog": rj(os.path.join(rp,"reflog.json"),[]), "seq": 0}}
try:
    with open(os.path.join(rp,"SEQ"), "r", encoding="utf-8") as f:
        bundle["seq"] = int((f.read().strip() or "0"))
except: pass
heads = os.path.join(rp,"refs","heads")
if os.path.isdir(heads):
    for name in os.listdir(heads):
        p = os.path.join(heads,name)
        if os.path.isfile(p):
            with open(p,"r",encoding="utf-8") as f:
                bundle["refs"][f"refs/heads/{{name}}"] = (f.read().splitlines()[:1] or [""])[0].strip()
cd = os.path.join(rp,"commits")
if os.path.isdir(cd):
    for fn in os.listdir(cd):
        if fn.endswith(".json"):
            bundle["commits"].append(rj(os.path.join(cd,fn),{{}}))
data = json.dumps({{"ok": True, "bundle": bundle}}, indent=2, ensure_ascii=False).encode("utf-8")
sys.stdout.write(base64.b64encode(data).decode("ascii"))
PY
"#, rp=sh_quote(&rp));

                let (code, so, se) = ssh_run(&user_host, &script)?;
                if code != 0 { bail!("ssh pull bundle failed: {}", se.trim()); }
                let bytes = base64::engine::general_purpose::STANDARD.decode(so.trim()).map_err(|e| anyhow!(e))?;
                let pr: PullResp = serde_json::from_slice(&bytes)?;
                if !pr.ok { bail!("pull response not ok"); }
                apply_bundle_local(Path::new("."), &pr.bundle)?;
            }
        }
    }

    let cache = Cache::default();
    let target_id = resolve_id_or_branch(&target)?;
    let commit = read_commit(&target_id)?;
    let files = commit_effective_map(&cache, &commit)?;
    restore_worktree(&files, remote_spec)?;

    let mut h = read_head()?;
    let head_ref_path = PathBuf::from(POP_DIR).join("refs").join("heads").join(&target);
    if head_ref_path.exists() {
        h.r#ref = Some(format!("refs/heads/{}", target));
        h.detached = None;
    } else {
        h.detached = Some(target_id.clone());
        h.r#ref = None;
    }
    write_head(&h)?;

    append_reflog(&current_commit_id()?, &target_id, "checkout")?;
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init => init_repo(),
        Cmd::Add { paths } => add_paths(paths),
        Cmd::Rm { paths } => rm_paths(paths),
        Cmd::Status => status_cmd(),
        Cmd::Commit { message } => commit_cmd(message),
        Cmd::Log { n } => log_cmd(n),
        Cmd::Checkout { target, remote, repo, token, ssh_root } => checkout_cmd(target, remote, repo, token, ssh_root),
        Cmd::Branch { name } => branch(name),
        Cmd::Diff { from, to } => diff_cmd(from, to),
        Cmd::Merge { branch } => merge_cmd(branch),
        Cmd::CherryPick { commit } => cherry_pick_cmd(commit),
        Cmd::Rebase { onto } => rebase_cmd(onto),
        Cmd::Reflog => reflog_cmd(),
        Cmd::Fsck => fsck_cmd(),
        Cmd::Gc => gc_cmd(),
        Cmd::Sparse { cmd } => match cmd {
            SparseCmd::Set { paths } => sparse_set(paths),
            SparseCmd::Clear => sparse_clear(),
            SparseCmd::Show => sparse_show(),
        },
        Cmd::Ignore { cmd } => match cmd {
            IgnoreCmd::Add { pattern } => ignore_add(pattern),
            IgnoreCmd::Show => ignore_show(),
        },
        #[cfg(feature="server")]
        Cmd::Serve(a) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(serve_cmd(a))
        }
        Cmd::Push(a) => push_remote_cmd(a),
        Cmd::Pull(a) => pull_remote_cmd(a),
        #[allow(unreachable_patterns)]
        _ => bail!("command not available in this build"),
    }
}
