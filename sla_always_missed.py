from prefect import flow, serve

@flow(name="SLA Always Missed")
def sla_always_missed():
    return "This flow will always miss its SLA"

if __name__ == "__main__":
    serve(sla_always_missed) 