# Prefect Flow Examples

This directory contains example Prefect flows demonstrating `flow.serve()` and `run_deployment()` patterns.

## Files

- `first_flow.py` - First flow that processes data
- `second_flow.py` - Second flow that triggers the first flow's deployment using `run_deployment()`
- `prefect.yaml` - Deployment configuration file

## Usage

### Option 1: Using prefect.yaml (Recommended)

Deploy all flows defined in `prefect.yaml`:

```bash
# Deploy all flows
prefect deploy --all

# Or deploy specific flows
prefect deploy -n first-flow-deployment
prefect deploy -n second-flow-deployment
```

Then trigger them:
```bash
prefect deployment run "first-flow/first-flow-deployment"
prefect deployment run "second-flow/second-flow-deployment"
```

### Option 2: Using flow.serve()

Run each flow file directly to serve it:

```bash
# Terminal 1 - Serve first flow
python first_flow.py

# Terminal 2 - Serve second flow
python second_flow.py
```

Then trigger them using the Prefect CLI or Python:

**Using Prefect CLI:**
```bash
prefect deployment run "first-flow/first-flow-deployment"
prefect deployment run "second-flow/second-flow-deployment"
```

**Using Python:**
```python
from prefect.deployments import run_deployment

# Trigger the first flow
run_deployment("first-flow/first-flow-deployment", parameters={"message": "Custom message"})

# Trigger the second flow
run_deployment("second-flow/second-flow-deployment")
```

## Key Concepts

- **`flow.serve()`**: Creates a deployment and starts a local process to handle flow runs
- **`run_deployment()`**: Programmatically triggers a deployment from within another flow
- **`prefect.yaml`**: Configuration file for defining deployments declaratively
- **Separate files**: Each flow in its own file allows independent deployment

## Requirements

- Prefect installed (`pip install prefect`)
- Prefect server running (automatically started when you run flows)
