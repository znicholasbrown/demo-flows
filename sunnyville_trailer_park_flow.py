"""
Sunnyville Trailer Park Flow - Community Management System

This flow demonstrates:
1. A master flow that orchestrates 100 resident management sub-flows
2. Each sub-flow invokes 2 task runs (check_lot_status and perform_maintenance)
3. Trailer park themed activities including lot inspections, maintenance, and community events

Theme: Managing daily operations at Sunnyville Trailer Park, a friendly community
where residents live the simple life in their mobile homes.
"""
import time
import random
from datetime import datetime
from typing import Dict, List

from prefect import flow, task
from prefect_dask import DaskTaskRunner


# ============================================================================
# TASKS - Trailer Park Operations
# ============================================================================

@task(tags=["inspection", "lot-management", "daily-operations"])
def check_lot_status(lot_number: int, resident_name: str) -> Dict:
    """
    Inspect a trailer lot for maintenance needs, violations, and general status.
    This is the first task run for each resident management flow.
    """
    print(f"🏠 Checking Lot #{lot_number} - Resident: {resident_name}")
    time.sleep(random.uniform(0.1, 0.3))

    # Random lot conditions
    grass_height = random.randint(2, 12)  # inches
    needs_mowing = grass_height > 6
    has_violation = random.random() < 0.15  # 15% chance of violation

    violations = []
    if has_violation:
        possible_violations = [
            "Unregistered vehicle in driveway",
            "Excessive lawn decorations",
            "Trash bins not stored properly",
            "Skirting needs repair",
            "Mailbox leaning",
            "Garden gnome collection exceeds limit (seriously, Gary?)"
        ]
        violations.append(random.choice(possible_violations))

    status = {
        "lot_number": lot_number,
        "resident": resident_name,
        "grass_height_inches": grass_height,
        "needs_mowing": needs_mowing,
        "has_violation": has_violation,
        "violations": violations,
        "inspection_time": datetime.now().isoformat(),
        "lot_condition": "needs_attention" if (needs_mowing or has_violation) else "good"
    }

    if needs_mowing:
        print(f"  🌱 Grass is {grass_height}\" tall - mowing needed")
    if has_violation:
        print(f"  ⚠️  Violation: {violations[0]}")
    else:
        print(f"  ✅ Lot is in good condition")

    return status


@task(tags=["maintenance", "repairs", "daily-operations"])
def perform_maintenance(lot_status: Dict) -> Dict:
    """
    Perform necessary maintenance based on lot inspection results.
    This is the second task run for each resident management flow.
    """
    lot_number = lot_status["lot_number"]
    resident = lot_status["resident"]

    print(f"🔧 Performing maintenance on Lot #{lot_number}")
    time.sleep(random.uniform(0.1, 0.3))

    actions_taken = []

    # Handle mowing if needed
    if lot_status["needs_mowing"]:
        print(f"  🚜 Mowing lawn (was {lot_status['grass_height_inches']}\" tall)")
        actions_taken.append("Lawn mowed")

    # Handle violations if present
    if lot_status["has_violation"]:
        print(f"  📋 Issuing citation for: {lot_status['violations'][0]}")
        actions_taken.append(f"Citation issued: {lot_status['violations'][0]}")

    # Random additional maintenance (30% chance)
    if random.random() < 0.3:
        extra_maintenance = random.choice([
            "Checked water hookup",
            "Inspected electrical panel",
            "Cleared drainage ditch",
            "Repaired lot number sign",
            "Pruned overhanging tree branches",
            "Fixed picnic table",
            "Replaced light bulb in common area"
        ])
        actions_taken.append(extra_maintenance)
        print(f"  🛠️  Extra maintenance: {extra_maintenance}")

    if not actions_taken:
        actions_taken.append("Routine inspection - no action needed")
        print(f"  ✨ No maintenance needed")

    maintenance_result = {
        **lot_status,
        "maintenance_completed": True,
        "actions_taken": actions_taken,
        "maintenance_time": datetime.now().isoformat(),
        "work_order_status": "completed"
    }

    return maintenance_result


# ============================================================================
# SUB-FLOW - Individual Resident Management
# ============================================================================

@flow(name="Resident Lot Management", log_prints=True)
def manage_resident_lot(lot_number: int, resident_name: str) -> Dict:
    """
    Sub-flow that manages a single trailer lot.
    Calls 2 tasks: check_lot_status and perform_maintenance.
    """
    print(f"\n{'='*60}")
    print(f"🏘️  Managing Lot #{lot_number} - {resident_name}")
    print(f"{'='*60}")

    # Task 1: Inspection
    lot_status = check_lot_status(lot_number, resident_name)

    # Task 2: Maintenance
    maintenance_result = perform_maintenance(lot_status)

    print(f"✅ Completed management for Lot #{lot_number}")

    return maintenance_result


# ============================================================================
# MASTER FLOW - Sunnyville Trailer Park Daily Operations
# ============================================================================

@flow(name="Sunnyville Trailer Park - Daily Operations", log_prints=True, task_runner=DaskTaskRunner())
def sunnyville_trailer_park_flow(num_lots: int = 100):
    """
    Master orchestration flow for daily trailer park operations.

    Manages operations for all 100 lots in Sunnyville Trailer Park:
    - Invokes 100 sub-flows (one per trailer lot/resident)
    - Each sub-flow runs 2 tasks (inspection + maintenance)
    - Total: 100 flows × 2 tasks = 200 task runs

    Architecture:
    - Main Flow: sunnyville_trailer_park_flow (Daily Operations)
      - Sub-Flow (100x): manage_resident_lot (per resident)
        - Task 1: check_lot_status (inspection)
        - Task 2: perform_maintenance (repairs/citations)

    Theme: Managing a friendly trailer park community where everyone
    knows everyone, lawn gnomes are abundant, and the mobile homes
    have more character than a Disney movie.
    """
    print("=" * 80)
    print("🏕️  SUNNYVILLE TRAILER PARK - DAILY OPERATIONS")
    print("=" * 80)
    print(f"📊 Total Lots: {num_lots}")
    print(f"🏠 Total Sub-Flows: {num_lots}")
    print(f"⚙️  Total Tasks: {num_lots * 2}")
    print("=" * 80)

    start_time = datetime.now()

    # Generate resident names for each lot
    first_names = [
        "Ricky", "Julian", "Bubbles", "Randy", "Lahey", "Cyrus", "J-Roc",
        "Sarah", "Lucy", "Trinity", "Barb", "Donna", "Ellen", "Sam",
        "Gary", "Terry", "Barry", "Larry", "Jerry", "Phil", "Cory",
        "Trevor", "Ray", "Jacob", "Dennis", "Donny", "Thomas", "Patrick",
        "Mike", "Steve", "Dave", "Joe", "Bob", "Bill", "Tom", "Jim",
        "Mary", "Sue", "Jane", "Ann", "Lisa", "Karen", "Linda", "Nancy",
        "Betty", "Ruth", "Helen", "Carol", "Judy", "Diane", "Paula",
        "Frank", "George", "Charlie", "Henry", "Walter", "Arthur", "Roy",
        "Albert", "Paul", "Carl", "Fred", "Earl", "Howard", "Ralph",
        "Eugene", "Leon", "Marvin", "Floyd", "Willie", "Ernest", "Dale",
        "Curtis", "Norman", "Vernon", "Wayne", "Donald", "Roger", "Gerald",
        "Kenneth", "Louis", "Russell", "Harold", "Danny", "Raymond", "Roy",
        "Johnny", "Timmy", "Jimmy", "Billy", "Bobby", "Ricky", "Donnie",
        "Gloria", "Doris", "Dorothy", "Virginia", "Mildred", "Frances", "Shirley"
    ]

    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
        "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson"
    ]

    # Generate unique resident names for each lot
    residents = []
    for i in range(num_lots):
        first = random.choice(first_names)
        last = random.choice(last_names)
        residents.append(f"{first} {last}")

    print(f"\n🚀 Starting daily operations for {num_lots} trailer lots...\n")

    # Execute 100 sub-flows in parallel (one for each lot)
    lot_results = []
    for lot_num in range(1, num_lots + 1):
        result = manage_resident_lot(lot_num, residents[lot_num - 1])
        lot_results.append(result)

    # Calculate summary statistics
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Aggregate results
    total_mowing_needed = sum(1 for r in lot_results if r["needs_mowing"])
    total_violations = sum(1 for r in lot_results if r["has_violation"])
    total_actions = sum(len(r["actions_taken"]) for r in lot_results)
    lots_good_condition = sum(1 for r in lot_results if r["lot_condition"] == "good")

    print("\n" + "=" * 80)
    print("🎯 SUNNYVILLE TRAILER PARK - DAILY OPERATIONS SUMMARY")
    print("=" * 80)
    print(f"⏱️  Operations Duration: {duration:.1f} seconds")
    print(f"🏠 Lots Inspected: {len(lot_results)}")
    print(f"🌱 Lots Needing Mowing: {total_mowing_needed}")
    print(f"⚠️  Violations Found: {total_violations}")
    print(f"✅ Lots in Good Condition: {lots_good_condition}")
    print(f"🔧 Total Maintenance Actions: {total_actions}")
    print(f"📊 Average Actions per Lot: {total_actions / len(lot_results):.2f}")
    print("=" * 80)
    print("🏕️  All residents cared for. Sunnyville Trailer Park is running smooth!")
    print("=" * 80)

    return {
        "total_lots": len(lot_results),
        "duration_seconds": duration,
        "mowing_needed": total_mowing_needed,
        "violations_found": total_violations,
        "lots_in_good_condition": lots_good_condition,
        "total_actions": total_actions,
        "lot_results": lot_results
    }


# Entry point for running the flow
if __name__ == "__main__":
    # Run with default 100 lots
    sunnyville_trailer_park_flow()
