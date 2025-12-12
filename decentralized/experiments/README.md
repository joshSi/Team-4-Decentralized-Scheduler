# Scheduler Comparison Framework

This framework compares the performance of centralized vs gossip (decentralized) schedulers for the serverless LLM cluster.

## Overview

The comparison framework runs experiments with identical workloads on both scheduler types and measures:

- **Resource Utilization**: Memory utilization distribution across workers
- **Queue Depth**: Request queue depth distribution
- **TTFT (Time to First Token)**: Latency from request arrival to first token generation
- **Request Latency**: Total request processing time
- **Success Rates**: Percentage of successfully completed requests
- **Action Distribution**: Breakdown of SERVE vs COLD_START vs MIGRATE actions
- **Recovery Time**: Time taken to recover from node failures (when applicable)

## Files

- `compare_schedulers.py` - Main comparison script
- `metrics_collector.py` - Metrics collection and aggregation module
- `README.md` - This documentation

## Usage

### Run Full Comparison (Both Schedulers)

```bash
cd /Users/josephjennings/Desktop/Folder/Research/Team-4-Decentralized-Scheduler/decentralized/experiments

python compare_schedulers.py \
  --duration 300 \
  --rps 5.0 \
  --cv 8.0 \
  --name my_comparison_experiment
```

This will:
1. Run centralized scheduler experiment for 300s
2. Wait 30s
3. Run gossip scheduler experiment for 300s
4. Generate comparison report
5. Save results to `/results/` directory

### Run Only Centralized Scheduler

```bash
python compare_schedulers.py --centralized-only --duration 300 --name centralized_test
```

### Run Only Gossip Scheduler

```bash
python compare_schedulers.py --gossip-only --duration 300 --name gossip_test
```

## Parameters

- `--duration` - Experiment duration in seconds (default: 300)
- `--rps` - Target requests per second (default: 5.0)
- `--cv` - Coefficient of variation for burstiness (default: 8.0)
- `--name` - Base name for the experiment (default: scheduler_comparison)
- `--centralized-only` - Run only centralized scheduler
- `--gossip-only` - Run only gossip scheduler

## Output

### Console Output

The script prints real-time progress and a final comparison report:

```
================================================================================
COMPARISON REPORT: CENTRALIZED VS GOSSIP
================================================================================

Metric                                   Centralized          Gossip               Winner
--------------------------------------------------------------------------------
Success Rate (%)                         95.2%                92.8%                Centralized
Avg TTFT (ms)                            125.45               138.22               Centralized
P95 TTFT (ms)                            215.32               245.67               Centralized
P99 TTFT (ms)                            287.45               312.89               Centralized
Avg Memory Util                          62.3%                58.1%                Centralized
P95 Memory Util                          78.9%                72.4%                Centralized
Avg Queue Depth                          2.45                 3.12                 Centralized
P95 Queue Depth                          8.00                 9.50                 Centralized
================================================================================
```

### CSV Files

Results are saved to:
- `/results/my_comparison_experiment_centralized_YYYYMMDD_HHMMSS.csv`
- `/results/my_comparison_experiment_gossip_YYYYMMDD_HHMMSS.csv`

CSV columns include all aggregated metrics:
- experiment_name, scheduler_type, start_time, end_time, duration
- total_requests, successful_requests, failed_requests
- avg_ttft, p50_ttft, p95_ttft, p99_ttft
- avg_latency, p50_latency, p95_latency, p99_latency
- avg_memory_utilization, min/max/p50/p95_memory_utilization
- avg_queue_depth, min/max/p50/p95_queue_depth
- avg_recovery_time
- serve_count, cold_start_count, migrate_count

## Architecture

### Centralized Scheduler Setup

```
┌─────────────┐
│ Load        │
│ Generator   │──┐
└─────────────┘  │ 1. Schedule Request
                 ▼
          ┌─────────────┐
          │ Central     │  Routes to optimal worker
          │ Coordinator │  (SERVE, COLD_START, MIGRATE)
          └──────┬──────┘
                 │ 2. Schedule Response
                 ▼
          ┌─────────────┐
          │ Worker-1/2/3│  Performs inference
          └─────────────┘
```

Docker Compose: `config/docker/docker-compose.yml`

Components:
- central-coordinator (port 9000/udp)
- worker-1, worker-2, worker-3 (port 8000)
- load-generator

### Gossip Scheduler Setup

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Worker-1    │◄───►│ Worker-2    │◄───►│ Worker-3    │
│ (Gossip)    │     │ (Gossip)    │     │ (Gossip)    │
└──────▲──────┘     └──────▲──────┘     └──────▲──────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                           │ Direct requests
                     ┌─────▼──────┐
                     │ Load       │
                     │ Generator  │
                     └────────────┘
```

Docker Compose: `config/docker/docker-compose-gossip.yml`

Components:
- gossip-worker-1, gossip-worker-2, gossip-worker-3 (inference + gossip protocol)
- gossip-load-generator

Workers communicate via UDP gossip protocol on ports 9001, 9002, 9003.

## Metrics Collection

The framework collects metrics by:

1. **Parsing Docker container logs** - Extracts request/response information from container stdout
2. **Polling worker state** - Periodically reads worker memory, queue depth, and readiness
3. **Computing percentiles** - Calculates P50, P95, P99 for latency and resource metrics
4. **Aggregating counts** - Tallies successful/failed requests and action types

Metrics are collected every 5 seconds during the experiment duration.

## Limitations & Notes

- **Gossip Implementation**: The current gossip mode uses a simplified approach where workers use UDP gossip for state dissemination, but scheduling is still done via round-robin from the load generator. A full gossip scheduler would have workers coordinate scheduling decisions via the gossip protocol.

- **CPU Inference**: Experiments run on CPU by default, which is very slow (~60s per request). For faster experiments, consider:
  - Reducing `--duration`
  - Lowering `--rps`
  - Using GPU-enabled workers (requires GPU support)

- **Model Loading**: Workers load `facebook/opt-125m` (125M parameters) which takes ~20-30s. The framework waits 60s after container startup before collecting metrics.

- **Recovery Testing**: Node failure recovery metrics are not yet implemented but are prepared in the schema.

## Example: Full Comparison Run

```bash
# Navigate to experiments directory
cd decentralized/experiments

# Run 5-minute comparison at 5 RPS with high burstiness
python compare_schedulers.py \
  --duration 300 \
  --rps 5.0 \
  --cv 8.0 \
  --name production_comparison_2025

# Results will be in:
# ../results/production_comparison_2025_centralized_*.csv
# ../results/production_comparison_2025_gossip_*.csv
```

## Troubleshooting

**Import errors when running script:**
```bash
# Make sure you're in the experiments directory
cd decentralized/experiments

# The script adds parent directories to Python path automatically
```

**Containers not found:**
```bash
# Verify Docker is running
docker ps

# Check that previous experiments are cleaned up
cd ../config/docker
docker-compose down
docker-compose -f docker-compose-gossip.yml down
```

**No metrics collected:**
- Increase the wait time in `run_centralized_experiment()` if workers are slow to load models
- Check container logs: `docker logs worker-1 --tail 100`

## Future Enhancements

- [ ] Implement true gossip-based scheduling (not just state dissemination)
- [ ] Add node failure injection and recovery time measurement
- [ ] Support GPU-accelerated inference
- [ ] Add visualization/plotting of results
- [ ] Implement real-time metrics dashboard during experiments
- [ ] Add support for heterogeneous model configurations
- [ ] Measure network bandwidth usage
