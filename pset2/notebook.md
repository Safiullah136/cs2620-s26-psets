# PSet 2 — Failure Injection Notebook

| | |
| --- | --- |
| **Seed** | `14780831915535655852` |
| **Command** | `./build/ctfail -S 14780831915535655852 -n 5` |

---

## Result

> **Consensus not achieved.**

Used a failure injection to break consensus.

---

## Changes

I made two changes to `ctconsensus.cc` and saved them to `ctfail.cc`:

### 1. Strict minority failure

Fail at most `N/2 - 1` servers (a strict minority) at some random point in the
sequence. Each candidate fails with a fixed probability.

### 2. Failure detector

Failure is detected on a timeout of **15 minutes**, with probability `0.1`.

---

## Viewing the diff

```bash
git diff --no-index ctconsensus.cc ctfail.cc
```