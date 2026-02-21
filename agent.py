import asyncio
import operator
import os
from typing import Annotated, Any, Dict, List, Optional

from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from hive_hook import (
    EndpointEnum,
    HiveInboundBaseData,
    hive_data,
    hive_hook,
    send_to_hive,
    start_server,
)


# ── Data Models ──────────────────────────────────────────────────────
class PaymentConfirmation(HiveInboundBaseData):
    amount: float
    currency: str


class ShippingLabel(HiveInboundBaseData):
    carrier: str
    tracking_number: str


class WarehouseAck(HiveInboundBaseData):
    warehouse_id: str


# ── Graph State ──────────────────────────────────────────────────────
class State(TypedDict):
    unique_id: Optional[str]
    messages: Annotated[List[str], operator.add]


# ── Graph Nodes ──────────────────────────────────────────────────────
async def validate_order(state: State) -> Dict[str, Any]:
    uid = state.get("unique_id", "unknown")

    print(f"[validate_order] validating {uid}")
    await asyncio.sleep(0.5)
    print(f"[validate_order] order {uid} is valid")

    await send_to_hive(
        destination_agent_id="billing_agent",
        destination_agent_endpoint=EndpointEnum.DATA,
        payload={
            "event": "order_validated",
            "order_id": uid,
        },
    )

    return {"messages": [f"order {uid} validated"]}


@hive_hook({"payment": PaymentConfirmation})
async def await_payment(state: State) -> Dict[str, Any]:
    payment = hive_data.get("payment", PaymentConfirmation)

    print(f"[await_payment] received {payment.amount} {payment.currency}")

    await send_to_hive(
        destination_agent_id="receipt_agent",
        destination_agent_endpoint=EndpointEnum.DATA,
        payload={
            "event": "payment_received",
            "amount": payment.amount,
            "currency": payment.currency,
        },
    )

    return {"messages": [f"paid {payment.amount} {payment.currency}"]}


@hive_hook(
    {
        "shipping_label": ShippingLabel,
        "warehouse_ack": WarehouseAck,
    }
)
async def await_fulfillment(state: State) -> Dict[str, Any]:
    label = hive_data.get("shipping_label", ShippingLabel)
    ack = hive_data.get("warehouse_ack", WarehouseAck)

    print(
        f"[await_fulfillment] label: {label.carrier} "
        f"{label.tracking_number}"
    )
    print(f"[await_fulfillment] warehouse: {ack.warehouse_id}")

    await send_to_hive(
        destination_agent_id="tracking_agent",
        destination_agent_endpoint=EndpointEnum.START,
        payload={
            "event": "shipment_ready",
            "carrier": label.carrier,
            "tracking_number": label.tracking_number,
            "warehouse": ack.warehouse_id,
        },
    )

    return {
        "messages": [
            f"shipped via {label.carrier}",
            f"packed at {ack.warehouse_id}",
        ]
    }


async def complete_order(state: State) -> Dict[str, Any]:
    uid = state.get("unique_id", "unknown")

    print(f"[complete_order] order {uid} complete — {state['messages']}")

    await send_to_hive(
        destination_agent_id="notification_agent",
        destination_agent_endpoint=EndpointEnum.DATA,
        payload={
            "event": "order_complete",
            "order_id": uid,
        },
    )

    return {"messages": ["done"]}


# ── Compile Graph ────────────────────────────────────────────────────
graph = StateGraph(State)

graph.add_node("validate_order", validate_order)
graph.add_node("await_payment", await_payment)
graph.add_node("await_fulfillment", await_fulfillment)
graph.add_node("complete_order", complete_order)

graph.set_entry_point("validate_order")

graph.add_edge("validate_order", "await_payment")
graph.add_edge("await_payment", "await_fulfillment")
graph.add_edge("await_fulfillment", "complete_order")
graph.add_edge("complete_order", END)

compiled = graph.compile()


# ── Entry point ──────────────────────────────────────────────────────
if __name__ == "__main__":
    start_server(
        compiled,
        agent_id=os.environ.get("HIVE_AGENT_ID", "pod1"),
    )
